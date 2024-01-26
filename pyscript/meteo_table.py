import datetime
import os
import time

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2._psycopg import OperationalError
from psycopg2.extras import execute_values
from meteoAPI import meteo_data_forecast
import smartworkingdays


def db_conn():
    # Risalire di una cartella rispetto alla directory corrente dello script
    base_dir = os.path.dirname(os.path.dirname(__file__))
    # Combinare il percorso base con la cartella 'env' e il nome del file '.env'
    env_path = os.path.join(base_dir, 'env', '.env')

    load_dotenv(env_path)
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')
    for _ in range(5):
        try:
            conn = psycopg2.connect(
                host=db_host,
                dbname=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )

            cur = conn.cursor()

            cur.execute("""
                    SELECT tablename FROM pg_catalog.pg_tables
                    WHERE schemaname = 'harpa';
                """)

            # Retrieve query results
            tables = cur.fetchall()
            print(f"Connection to {db_name} established, tables:{tables}")
            return [tables, cur, conn]
        except OperationalError as e:
            print(f"Errore: {e}"
                  f"Connessione fallita, nuovo tentativo in {10} secondi.")
            time.sleep(10)


def update_tables_meteo_data(tables, cur, conn):
    sorted_tables = sorted(tables, key=lambda x: (x[0] != 'aggregazione_giorno', x[0]))
    meteo_data = None
    for table in sorted_tables:
        if table[0] == 'aggregazione_ora':
            try:
                cur.execute(f"SELECT ora FROM HARPA.{table[0]}")
                records = cur.fetchall()
                start_date = records[0][0]
                end_date = records[-1][0]

                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                meteo_data = meteo_data_forecast(start_date_str, end_date_str,
                                                 False, None) if meteo_data is None else meteo_data

                # Controllare le colonne esistenti
                cur.execute("SELECT * FROM HARPA.aggregazione_ora LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

                # Aggiungere le colonne se non esistono
                columns_to_add = ['solo_ora', 'giorno', 'fascia_oraria', 'temperature', 'rain', 'cloud_cover',
                                  'relative_humidity_2m',
                                  'wind_speed_10m',
                                  'wind_direction_10m']
                columns_to_alter = [col for col in columns_to_add if col not in col_names]
                for col in columns_to_alter:
                    if col == 'fascia_oraria':
                        alter_table_query = f"ALTER TABLE HARPA.aggregazione_ora ADD COLUMN {col} text;"
                    elif col == 'giorno':
                        alter_table_query = f"ALTER TABLE HARPA.aggregazione_ora ADD COLUMN {col} date;"
                    elif col == 'solo_ora':
                        alter_table_query = f"ALTER TABLE HARPA.aggregazione_ora ADD COLUMN {col} time;"
                    else:
                        alter_table_query = f"ALTER TABLE HARPA.aggregazione_ora ADD COLUMN {col} NUMERIC(10, 5);"
                    cur.execute(alter_table_query)

                batch_update_data = []
                for record in records:
                    data_ora_completa = record[0]
                    solo_ora_str = data_ora_completa.strftime("%H:%M:%S")  # Estrai solo l'ora come stringa
                    solo_ora = datetime.datetime.strptime(solo_ora_str, "%H:%M:%S").time()  # Converti in oggetto time
                    giorno_str = data_ora_completa.strftime("%Y-%m-%d")  # Estrai solo il giorno come stringa
                    giorno = datetime.datetime.strptime(giorno_str, "%Y-%m-%d").date()
                    fascia = categorizza_fascia_oraria(data_ora_completa.time())
                    batch_update_data.append((giorno, fascia, solo_ora, data_ora_completa))

                # Creare una query di aggiornamento batch
                batch_update_query = """
                    UPDATE HARPA.aggregazione_ora
                    SET solo_ora = data.solo_ora, giorno = data.giorno, fascia_oraria = data.fascia_oraria
                    FROM (VALUES %s) AS data(giorno, fascia_oraria, solo_ora, data_ora_completa)
                    WHERE HARPA.aggregazione_ora.ora = data.data_ora_completa;
                """
                execute_values(cur, batch_update_query, batch_update_data)
                # Preparaew i dati per l'aggiornamento batch
                update_data = []
                for index, row in meteo_data.iterrows():
                    update_data.append((row['temperature_2m'], row['rain'], row['cloud_cover'],
                                        row['relative_humidity_2m'], row['wind_speed_10m'], row['wind_direction_10m'],
                                        row['date']))

                # Eseguire l'aggiornamento batch
                update_query = """
                    UPDATE HARPA.aggregazione_ora
                    SET temperature = data.temperature_2m,
                        rain = data.rain,
                        cloud_cover = data.cloud_cover,
                        relative_humidity_2m = data.relative_humidity_2m,
                        wind_speed_10m = data.wind_speed_10m,
                        wind_direction_10m = data.wind_direction_10m
                    FROM (VALUES %s) AS data(temperature_2m, rain, cloud_cover, 
                    relative_humidity_2m, wind_speed_10m, wind_direction_10m, date)
                    WHERE ora = data.date;
                """
                try:
                    execute_values(cur, update_query, update_data)
                    conn.commit()
                    print(f"Table {table[0]} correctly updated ")
                except Exception as e:
                    print(f"Errore durante l'aggiornamento: {e}")
                    conn.rollback()

            except psycopg2.Error as e:
                print(f"Error querying table {table[0]}: {e}")

                # Commit the changes
                conn.commit()
        elif table[0] == 'aggregazione_fascia_oraria':
            try:
                cur.execute(f"SELECT DISTINCT giorno FROM HARPA.{table[0]} ORDER BY giorno")
                records = cur.fetchall()
                start_date = records[0][0]
                end_date = records[-1][0]

                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                meteo_data = meteo_data_forecast(start_date_str,
                                                 end_date_str,
                                                 False,
                                                 None) if meteo_data is None else meteo_data
                meteo_data_fascia_oraria = meteo_data
                meteo_data_fascia_oraria['date'] = pd.to_datetime(meteo_data_fascia_oraria['date'])
                meteo_data_fascia_oraria['only_date'] = meteo_data_fascia_oraria['date'].dt.date
                meteo_data_fascia_oraria['only_time'] = meteo_data_fascia_oraria['date'].dt.time
                meteo_data_fascia_oraria['fascia_oraria'] = meteo_data_fascia_oraria['only_time'].apply(
                    categorizza_fascia_oraria)
                colonne_numeriche = meteo_data_fascia_oraria.select_dtypes(include=['number'])
                meteo_data_aggregata = (colonne_numeriche.groupby([meteo_data_fascia_oraria['only_date'],
                                                                   meteo_data_fascia_oraria['fascia_oraria']])
                                        .mean().reset_index())

                # Controllare le colonne esistenti
                cur.execute("SELECT * FROM HARPA.aggregazione_fascia_oraria LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

                # Aggiungere le colonne se non esistono
                columns_to_add = ['temperature', 'rain', 'cloud_cover', 'relative_humidity_2m', 'wind_speed_10m',
                                  'wind_direction_10m']
                columns_to_alter = [col for col in columns_to_add if col not in col_names]
                for col in columns_to_alter:
                    alter_table_query = f"ALTER TABLE HARPA.aggregazione_fascia_oraria ADD COLUMN {col} NUMERIC(10, 5);"
                    cur.execute(alter_table_query)

                # Preparare i dati per l'aggiornamento batch
                update_data = []
                for index, row in meteo_data_aggregata.iterrows():
                    update_data.append((row['temperature_2m'],
                                        row['rain'], row['cloud_cover'],
                                        row['relative_humidity_2m'],
                                        row['wind_speed_10m'],
                                        row['wind_direction_10m'],
                                        row['only_date'],
                                        row['fascia_oraria']))
                # Eseguire l'aggiornamento batch
                update_query = """
                    UPDATE HARPA.aggregazione_fascia_oraria
                    SET temperature = data.temperature_2m,
                        rain = data.rain,
                        cloud_cover = data.cloud_cover,
                        relative_humidity_2m = data.relative_humidity_2m,
                        wind_speed_10m = data.wind_speed_10m,
                        wind_direction_10m = data.wind_direction_10m
                    FROM (VALUES %s) AS data(temperature_2m, rain, cloud_cover, relative_humidity_2m, 
                    wind_speed_10m, wind_direction_10m, only_date, fascia_oraria)
                    WHERE HARPA.aggregazione_fascia_oraria.giorno = data.only_date
                    AND HARPA.aggregazione_fascia_oraria.fascia_oraria = data.fascia_oraria;
                """
                try:
                    execute_values(cur, update_query, update_data)
                    # print(cur.mogrify(update_query, [update_data]).decode('utf-8'))
                    conn.commit()
                    print(f"Table {table[0]} correctly updated ")
                except Exception as e:
                    print(f"Errore durante l'aggiornamento: {e}")
                    conn.rollback()

            except psycopg2.Error as e:
                print(f"Error querying table {table[0]}: {e}")
        elif table[0] == 'aggregazione_giorno':
            try:
                cur.execute(f"SELECT DISTINCT giorno FROM HARPA.{table[0]} ORDER BY giorno")
                records = cur.fetchall()
                start_date = records[0][0]
                end_date = records[-1][0]

                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                meteo_data = meteo_data_forecast(start_date_str,
                                                 end_date_str,
                                                 False,
                                                 None) if meteo_data is None else meteo_data
                meteo_data_giorno = meteo_data
                meteo_data_giorno['date'] = pd.to_datetime(meteo_data_giorno['date'])
                meteo_data_giorno['only_date'] = meteo_data_giorno['date'].dt.date
                colonne_numeriche = meteo_data_giorno.select_dtypes(include=['number'])
                meteo_data_aggregata = (colonne_numeriche.groupby([meteo_data_giorno['only_date']])
                                        .mean().reset_index())

                # Controllare le colonne esistenti
                cur.execute("SELECT * FROM HARPA.aggregazione_giorno LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

                # Aggiungere le colonne se non esistono
                columns_to_add = ['temperature', 'rain', 'cloud_cover', 'relative_humidity_2m', 'wind_speed_10m',
                                  'wind_direction_10m']
                columns_to_alter = [col for col in columns_to_add if col not in col_names]
                for col in columns_to_alter:
                    alter_table_query = f"ALTER TABLE HARPA.aggregazione_giorno ADD COLUMN {col} NUMERIC(10, 5);"
                    cur.execute(alter_table_query)

                # Preparare i dati per l'aggiornamento batch
                update_data = []
                for index, row in meteo_data_aggregata.iterrows():
                    update_data.append((row['temperature_2m'],
                                        row['rain'], row['cloud_cover'],
                                        row['relative_humidity_2m'],
                                        row['wind_speed_10m'],
                                        row['wind_direction_10m'],
                                        row['only_date']))
                # Eseguire l'aggiornamento batch
                update_query = """
                    UPDATE HARPA.aggregazione_giorno  
                    SET temperature = data.temperature_2m,
                        rain = data.rain,
                        cloud_cover = data.cloud_cover,
                        relative_humidity_2m = data.relative_humidity_2m,
                        wind_speed_10m = data.wind_speed_10m,
                        wind_direction_10m = data.wind_direction_10m
                    FROM (VALUES %s) AS data(temperature_2m, rain, cloud_cover, relative_humidity_2m, 
                    wind_speed_10m, wind_direction_10m, only_date)
                    WHERE HARPA.aggregazione_giorno.giorno = data.only_date;
                """

                try:
                    execute_values(cur, update_query, update_data)
                    # print(cur.mogrify(update_query, [update_data]).decode('utf-8'))
                    conn.commit()
                    print(f"Table {table[0]} correctly updated ")
                except Exception as e:
                    print(f"Errore durante l'aggiornamento: {e}")
                    conn.rollback()

            except psycopg2.Error as e:
                print(f"Error querying table {table[0]}: {e}")
        elif table[0] == 'aggregazione_mese':
            try:
                meteo_data_mese = meteo_data
                meteo_data_mese['date'] = pd.to_datetime(meteo_data_mese['date'])
                meteo_data_mese['year_month'] = meteo_data_mese['date'].dt.strftime('%Y-%m')
                colonne_numeriche = meteo_data_mese.select_dtypes(include=['number'])
                meteo_data_aggregata = (colonne_numeriche.groupby([meteo_data_mese['year_month']])
                                        .mean().reset_index())

                # Controllare le colonne esistenti
                cur.execute("SELECT * FROM HARPA.aggregazione_mese LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

                # Aggiungere le colonne se non esistono
                columns_to_add = ['temperature', 'rain', 'cloud_cover', 'relative_humidity_2m', 'wind_speed_10m',
                                  'wind_direction_10m']
                columns_to_alter = [col for col in columns_to_add if col not in col_names]
                for col in columns_to_alter:
                    alter_table_query = f"ALTER TABLE HARPA.aggregazione_mese ADD COLUMN {col} NUMERIC(10, 5);"
                    cur.execute(alter_table_query)

                # Preparare i dati per l'aggiornamento batch
                update_data = []
                for index, row in meteo_data_aggregata.iterrows():
                    update_data.append((row['temperature_2m'],
                                        row['rain'], row['cloud_cover'],
                                        row['relative_humidity_2m'],
                                        row['wind_speed_10m'],
                                        row['wind_direction_10m'],
                                        row['year_month']))
                # Eseguire l'aggiornamento batch
                update_query = """
                    UPDATE HARPA.aggregazione_mese  
                    SET temperature = data.temperature_2m,
                        rain = data.rain,
                        cloud_cover = data.cloud_cover,
                        relative_humidity_2m = data.relative_humidity_2m,
                        wind_speed_10m = data.wind_speed_10m,
                        wind_direction_10m = data.wind_direction_10m
                    FROM (VALUES %s) AS data(temperature_2m, rain, cloud_cover, relative_humidity_2m, wind_speed_10m,
                     wind_direction_10m,year_month)
                    WHERE HARPA.aggregazione_mese.mese = data.year_month;
                """

                try:
                    execute_values(cur, update_query, update_data)
                    # print(cur.mogrify(update_query, [update_data]).decode('utf-8'))
                    conn.commit()
                    print(f"Table {table[0]} correctly updated ")
                except Exception as e:
                    print(f"Errore durante l'aggiornamento: {e}")
                    conn.rollback()
            except psycopg2.Error as e:
                print(f"Error querying table {table[0]}: {e}")
        elif table[0] == 'aggregazione_anno':
            try:
                meteo_data_anno = meteo_data
                meteo_data_anno['date'] = pd.to_datetime(meteo_data_anno['date'])
                meteo_data_anno['year'] = meteo_data_anno['date'].dt.year
                colonne_numeriche = meteo_data_anno.select_dtypes(include=['number'])
                meteo_data_aggregata = colonne_numeriche.groupby([meteo_data_anno['year']]).mean().reset_index(
                    drop=True)
                meteo_data_aggregata['year'] = meteo_data_aggregata['year'].astype(int)

                # Controllare le colonne esistenti
                cur.execute("SELECT * FROM HARPA.aggregazione_anno LIMIT 0")
                col_names = [desc[0] for desc in cur.description]

                # Aggiungere le colonne se non esistono
                columns_to_add = ['temperature', 'rain', 'cloud_cover', 'relative_humidity_2m', 'wind_speed_10m',
                                  'wind_direction_10m']
                columns_to_alter = [col for col in columns_to_add if col not in col_names]
                for col in columns_to_alter:
                    alter_table_query = f"ALTER TABLE HARPA.aggregazione_anno ADD COLUMN {col} NUMERIC(10, 5);"
                    cur.execute(alter_table_query)

                # Preparare i dati per l'aggiornamento batch
                update_data = []
                for index, row in meteo_data_aggregata.iterrows():
                    update_data.append((row['temperature_2m'],
                                        row['rain'], row['cloud_cover'],
                                        row['relative_humidity_2m'],
                                        row['wind_speed_10m'],
                                        row['wind_direction_10m'],
                                        row['year']))
                # Eseguire l'aggiornamento batch
                update_query = """
                    UPDATE HARPA.aggregazione_anno  
                    SET temperature = data.temperature_2m,
                        rain = data.rain,
                        cloud_cover = data.cloud_cover,
                        relative_humidity_2m = data.relative_humidity_2m,
                        wind_speed_10m = data.wind_speed_10m,
                        wind_direction_10m = data.wind_direction_10m
                    FROM (VALUES %s) AS data(temperature_2m, rain, cloud_cover, relative_humidity_2m, wind_speed_10m,
                     wind_direction_10m,year)
                    WHERE HARPA.aggregazione_anno.anno = data.year;
                """

                try:
                    execute_values(cur, update_query, update_data)
                    conn.commit()
                    print(f"Table {table[0]} correctly updated ")
                except Exception as e:
                    print(f"Errore durante l'aggiornamento: {e}")
                    conn.rollback()
            except psycopg2.Error as e:
                print(f"Error querying table {table[0]}: {e}")
    # Close the cursor and connection
    smartworkingdays.smartworking_insert(cur)
    cur.close()
    conn.close()


def categorizza_fascia_oraria(orario):
    if orario < pd.Timestamp('09:00').time():
        return '00:00-09:00'
    elif orario < pd.Timestamp('18:00').time():
        return '09:00-18:00'
    else:
        return '18:00-00:00'
