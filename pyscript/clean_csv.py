import os
import pandas as pd
from insert_update import update_tables_meteo_data
from smartworkingdays import smartworking_insert
from utils import csv_align_date, read_json_file, check_date_in_json, write_json_file, update_table

json_file_cache = 'cache.json'
json_directory_cache = "../cache/"
full_cache_path = os.path.join(json_directory_cache, json_file_cache)


def clean(logging, cur, conn):
    csv_files = [
        "../Dataset/Generale_Data_Center_Energia_Attiva.csv",
        "../Dataset/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.csv",
        "../Dataset/Generale_Edificio_Energia_Attiva.csv",
    ]
    csv_align_date(csv_files)
    start = None
    end = None
    update = False
    exists = False
    for index, file_path in enumerate(csv_files):
        new_file_path = file_path.replace(".csv", "_no_duplicates.csv")
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
            logging.info(f"Vecchio file {new_file_path} eliminato.")

        df = pd.read_csv(file_path, encoding='utf-8')
        df_no_duplicates = df.drop_duplicates(subset=['Date'])

        df_no_duplicates.loc[:, 'Date'] = pd.to_datetime(df_no_duplicates['Date'])
        start = df_no_duplicates['Date'].min().to_pydatetime()
        end = df_no_duplicates['Date'].max().to_pydatetime()

        if os.path.exists(json_directory_cache):
            dati = read_json_file(full_cache_path)
            start_json, end_json = check_date_in_json(dati, start, end)
            end_json = pd.to_datetime(end_json)
            exists = True
            if index == len(csv_files) - 1:
                logging.info("Salvo ultima data in cache...")
                write_json_file(start, end)
            if end != end_json:
                update = True
                df_no_duplicates = df_no_duplicates[(df_no_duplicates['Date'] > end_json)]
        else:
            update = True
            if index == len(csv_files) - 1:
                logging.info("Salvo ultima data in cache...")
                write_json_file(start, end)

        df_no_duplicates.to_csv(new_file_path, index=False, sep=',', float_format='%.2f', encoding='utf-8')
        logging.info(f'File salvato senza duplicati: {new_file_path}')

    if update:
        update_table(logging, cur, conn)
        update_tables_meteo_data(logging, cur, conn, start, end)
        smartworking_insert(cur, conn, logging,exists)
    else:
        logging.warn("Non ci sono nuove date nei csv forniti, nessuna tabella da aggiornare")
    cur.close()
    conn.close()



