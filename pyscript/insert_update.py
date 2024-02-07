import json
import logging
import os
import traceback
from datetime import datetime
import pandas as pd
import psycopg2
from pyscript.meteoAPI import meteo_data_forecast
from pyscript.smartworkingdays import smartworking_insert

log_directory = '../log/'
log_filename = 'meteo_update.log'
full_log_path = os.path.join(log_directory, log_filename)
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
# Configura il logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()])


def log_and_execute(cur, query, params, table_name, conn):
    try:
        cur.execute(query, params)
        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Errore durante l'aggiornamento della tabella {table_name}: {e}")


def update_tables_meteo_data(tables, cur, conn):
    start, end = fetch_date_range(tables, cur)
    meteo_data = fetch_meteo_data(start, end)
    if meteo_data is not None:
        process_and_update(meteo_data, cur, conn)
        smartworking_insert(cur, logging)
        cur.execute("""
        DROP TABLE IF EXISTS HARPA.edificio_staging;
        DROP TABLE IF EXISTS HARPA.data_center_staging;
        DROP TABLE IF EXISTS HARPA.fotovoltaico_staging;
        """)
        logging.info("Process finished drop staging tables")
    conn.close()


def fetch_date_range(tables, cur):
    target_table = 'edificio_staging' if 'edificio_staging' in tables else 'edificio'
    cur.execute(f"SELECT data FROM HARPA.{target_table} ORDER BY data")
    records = cur.fetchall()

    if records:
        start_date = records[0][0]
        end_date = records[-1][0]
        logging.info(f'Recupero date dalla tabella: {target_table}')
        return start_date, end_date
    else:
        logging.error(f'Errore nel recupero date dalla tabella: {target_table}')
        return None, None


def read_json_file(file_path):
    if os.path.isfile(file_path):
        with open(file_path, "r") as file_json:
            return json.load(file_json)
    else:
        return []


def write_json_file(file_path, data):
    with open(file_path, "w") as file_json:
        json.dump(data, file_json, indent=4)


def ensure_directory_exists(json_directory, full_cache_path):
    if not os.path.exists(json_directory):
        os.makedirs(json_directory)
        dati = []
        nuovo_dato = {
            "data_di_download": str(datetime.today()),
            "data_inizio": '',
            "data_fine": ''
        }
        dati.append(nuovo_dato)
        write_json_file(full_cache_path, dati)
        logging.info(f"Cartella '{json_directory}' creata.")


def fetch_meteo_data(start, end):
    json_file = 'cache.json'
    json_directory = "../cache/"
    full_cache_path = os.path.join(json_directory, json_file)
    ensure_directory_exists(json_directory, full_cache_path)

    dati = read_json_file(full_cache_path)
    start_json, end_json = check_date_in_json(dati, start, end)

    if start != start_json and end != end_json:
        meteo_data = meteo_data_forecast(start, end, False, None)
        if not meteo_data.empty:
            # Aggiorna il file JSON solo se i dati meteo sono stati recuperati con successo
            nuovo_dato = {
                "data_di_download": str(datetime.today()),
                "data_inizio": str(start),
                "data_fine": str(end)
            }
            dati.append(nuovo_dato)
            write_json_file(full_cache_path, dati)
            logging.info(f"Nuovo dato aggiunto al file JSON '{full_cache_path}'.")
        return meteo_data
    else:
        write_json_file(full_cache_path, dati)
        logging.info("Le date nel JSON sono aggiornate. Nessun nuovo dato da scaricare.")
        return None


def check_date_in_json(dati, start, end):
    if dati:
        ultimo_record = dati[-1]
        start_json = datetime.strptime(ultimo_record.get("data_inizio"), '%Y-%m-%d %H:%M:%S') if ultimo_record.get(
            "data_inizio") != '' else ultimo_record.get("data_inizio")
        end_json = datetime.strptime(ultimo_record.get("data_fine"), '%Y-%m-%d %H:%M:%S') if ultimo_record.get(
            "data_fine") != '' else ultimo_record.get("data_inizio")
        return start_json, end_json
    return start, end


def process_and_update(df, cur, conn):
    df = df.rename(columns={'date': 'data'})
    df['data'] = pd.to_datetime(df['data'])

    # Assegna ogni riga a una fascia oraria
    df['fascia_oraria'] = df['data'].dt.hour.apply(assign_time_slot)

    # Aggrega i dati per giorno e calcola la media
    agg_ora = df.resample('H', on='data').mean().reset_index()

    # Aggrega i dati per giorno e calcola la media
    agg_giorno = df.resample('D', on='data').mean().reset_index()
    agg_giorno['data'] = agg_giorno['data'].dt.date

    # Aggrega i dati per mese e imposta la data al primo del mese
    agg_mese = df.resample('M', on='data').mean().reset_index()
    agg_mese['data'] = agg_mese['data'].dt.to_period('M').dt.to_timestamp()

    # Aggrega i dati per anno e imposta la data al primo dell'anno
    agg_anno = df.resample('Y', on='data').mean().reset_index()
    agg_anno['data'] = agg_anno['data'].dt.to_period('Y').dt.to_timestamp()

    # Aggrega i dati per fascia oraria e calcola la media
    agg_fascia_oraria = df.groupby(['data', 'fascia_oraria']).mean().reset_index()
    agg_fascia_oraria['data'] = agg_fascia_oraria['data'].dt.date

    # Procedi con l'aggiornamento delle aggregazioni
    update_aggregations([agg_ora, agg_giorno, agg_mese, agg_anno, agg_fascia_oraria], cur,
                        ['ora', 'giorno', 'mese', 'anno', 'fascia_oraria'], conn)


def assign_time_slot(hour):
    if 0 <= hour < 9:  # Dalle 00:00 (incluso) alle 09:00 (escluso)
        return 1
    elif 9 <= hour < 19:  # Dalle 09:00 (incluso) alle 19:00 (escluso)
        return 2
    else:
        return 3  # Dalle 19:00 (incluso) in poi


def update_aggregations(aggregations, cur, table_names, conn):
    for agg, table_name in zip(aggregations, table_names):
        try:
            for index, row in agg.iterrows():
                if table_name == 'fascia_oraria':
                    params = (
                        row['data'], row['fascia_oraria'], row['temperature_2m'], row['rain'],
                        row['cloud_cover'], row['relative_humidity_2m'], row['wind_speed_10m'],
                        row['wind_direction_10m']
                    )
                else:
                    params = (
                        row['data'], row['temperature_2m'], row['rain'], row['cloud_cover'],
                        row['relative_humidity_2m'], row['wind_speed_10m'], row['wind_direction_10m']
                    )
                query = generate_query(table_name)
                log_and_execute(cur, query, params, f"HARPA.aggregazione_{table_name}", conn)
            logging.info(f"Aggiornata tabella {table_name} con successo.")
        except Exception as e:
            logging.error(f'Errore durante l\'aggiornamento della tabella {table_name}: {e}')
            logging.error(traceback.format_exc())


def generate_query(table_name):
    if table_name == 'fascia_oraria':
        columns = "data, fascia_oraria, temperature_2m, rain, cloud_cover, relative_humidity_2m, wind_speed_10m, wind_direction_10m"
        conflict_columns = "(data, fascia_oraria)"
    else:
        columns = "data, temperature_2m, rain, cloud_cover, relative_humidity_2m, wind_speed_10m, wind_direction_10m"
        conflict_columns = "(data)"

    placeholders = ', '.join(['%s'] * (columns.count(',') + 1))
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns.split(", ")[1:]])

    query = f"""
        INSERT INTO HARPA.aggregazione_{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT {conflict_columns} DO UPDATE SET
        {update_clause};
    """
    return query
