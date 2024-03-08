import traceback
import pandas as pd
from meteoAPI import meteo_data_forecast


def update_tables_meteo_data(logging, cur, conn, start, end):
    meteo_data = fetch_meteo_data(logging, start, end)
    if meteo_data is not None:
        try:
            process_and_update(logging, meteo_data, cur, conn)
            logging.info("Aggiornati i dati meteo!")
        except Exception as e:
            logging.error(f"Errore durante l'inserimento dei dati meteo nella tabella: {e}")


def fetch_meteo_data(logging, start, end):
    try:
        logging.info("Scarico i nuovi dati meteo")
        return meteo_data_forecast(start, end, False, None)
    except Exception as e:
        logging.error(f"Errore nello scaricare i nuovi dati: {e}")


def process_and_update(logging, df, cur, conn):
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
    update_aggregations(logging, [agg_ora, agg_giorno, agg_mese, agg_anno, agg_fascia_oraria], cur,
                        ['ora', 'giorno', 'mese', 'anno', 'fascia_oraria'], conn)


def update_aggregations(logging, aggregations, cur, table_names, conn):
    try:
        for agg, table_name in zip(aggregations, table_names):
            for index, row in agg.iterrows():
                if table_name == 'fascia_oraria':
                    params = (
                        row['data'], row['fascia_oraria'], row['temperature_2m'],
                        row['relative_humidity_2m'], row['dew_point_2m'],
                        row['apparent_temperature'], row['precipitation'], row['rain'],
                        row['snowfall'], row['snow_depth'], row['weather_code'],
                        row['pressure_msl'], row['surface_pressure'], row['cloud_cover'],
                        row['cloud_cover_low'], row['cloud_cover_mid'], row['cloud_cover_high'],
                        row['wind_speed_10m'], row['wind_direction_10m']
                    )
                else:
                    params = (
                        row['data'], row['temperature_2m'], row['relative_humidity_2m'],
                        row['dew_point_2m'], row['apparent_temperature'], row['precipitation'],
                        row['rain'], row['snowfall'], row['snow_depth'],
                        row['weather_code'], row['pressure_msl'], row['surface_pressure'], row['cloud_cover'],
                        row['cloud_cover_low'], row['cloud_cover_mid'], row['cloud_cover_high'],
                        row['wind_speed_10m'], row['wind_direction_10m']
                    )

                query = generate_query(table_name)
                log_and_execute(cur, query, params, conn)
            logging.info(f"Aggiornata tabella {table_name} con successo.")
    except Exception as e:
        logging.error(f'Errore durante l\'aggiornamento della tabella: {e}')
        logging.error(traceback.format_exc())


def generate_query(table_name):
    if table_name == 'fascia_oraria':
        columns = ("data, fascia_oraria, temperature_2m, relative_humidity_2m, dew_point_2m, "
                   "apparent_temperature, precipitation, rain, snowfall, snow_depth, "
                   "weather_code, pressure_msl, surface_pressure, cloud_cover, "
                   "cloud_cover_low, cloud_cover_mid, cloud_cover_high, wind_speed_10m, "
                   "wind_direction_10m")
        conflict_columns = "(data, fascia_oraria)"
    else:
        columns = ("data, temperature_2m, relative_humidity_2m, dew_point_2m, "
                   "apparent_temperature, precipitation, rain, snowfall, snow_depth, "
                   "weather_code, pressure_msl, surface_pressure, cloud_cover, "
                   "cloud_cover_low, cloud_cover_mid, cloud_cover_high, wind_speed_10m, "
                   "wind_direction_10m")

        conflict_columns = "(data)"

    placeholders = ', '.join(['%s'] * (columns.count(',') + 1))
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns.split(",")])

    query = f"""
        INSERT INTO HARPA.aggregazione_{table_name} ({columns})
        VALUES ({placeholders})
        ON CONFLICT {conflict_columns} DO UPDATE SET
        {update_clause};
    """
    return query


def log_and_execute(cur, query, params, conn):
    cur.execute(query, params)
    conn.commit()


def assign_time_slot(hour):
    if 0 <= hour < 9:  # Dalle 00:00 (incluso) alle 09:00 (escluso)
        return 1
    elif 9 <= hour < 19:  # Dalle 09:00 (incluso) alle 19:00 (escluso)
        return 2
    else:
        return 3  # Dalle 19:00 (incluso) in poi
