import os
import pandas as pd
from datetime import datetime
import json

json_file_cache = 'cache.json'
json_directory_cache = "../cache/"
full_cache_path = os.path.join(json_directory_cache, json_file_cache)


def clean_date_format(df):
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S')
    return df


def write_json_file(start, end):
    # Assicurati che la cartella esista
    if not os.path.exists(json_directory_cache):
        os.makedirs(json_directory_cache)
    data = []
    # Se il file esiste già, carico i dati esistenti
    if os.path.exists(full_cache_path):
        with open(full_cache_path, "r") as file_json:
            data = json.load(file_json)

    # Aggiungi i nuovi dati
    nuovo_dato = {
        "data_di_download": str(datetime.today()),
        "data_inizio": str(start),
        "data_fine": str(end)
    }
    data.append(nuovo_dato)

    # Salva l'elenco aggiornato nel file JSON
    with open(full_cache_path, "w") as file_json:
        json.dump(data, file_json, indent=4)


def update_table(logging, cur, conn):
    try:
        with open("../init/init-db.sql", "r") as file:
            sql_script = file.read()
            sql_statements = sql_script.split(';')
            cur.execute(sql_script)
            for statement in sql_statements:
                try:
                    if statement.strip():
                        cur.execute(statement)
                        logging.info(f"Istruzione eseguita con successo: {statement}")
                except Exception as e:
                    logging.error(f"Errore durante l'esecuzione dell'istruzione: {statement}. Errore: {e}")
                    break
            conn.commit()
    except Exception as e:
        logging.error(f"Errore durante l'esecuzione dello script init-db: {e}")


def get_ultima_date(csv_file):
    df = pd.read_csv(csv_file)
    ultima_data = df['Date'].max()
    return ultima_data


def csv_align_date(csv):
    ultima_data_meno_recente = None
    for csv_file in csv:
        ultima_data_csv = get_ultima_date(csv_file)
        if ultima_data_meno_recente is None or ultima_data_csv < ultima_data_meno_recente:
            ultima_data_meno_recente = ultima_data_csv

    for csv_file in csv:
        df = pd.read_csv(csv_file)
        df = df[df['Date'] <= ultima_data_meno_recente]
        df.to_csv(csv_file, index=False)


def read_json_file(file_path):
    if os.path.isfile(file_path):
        with open(file_path, "r") as file_json:
            return json.load(file_json)
    else:
        return []


def check_date_in_json(dati, start, end):
    if dati:
        ultimo_record = dati[-1]
        start_json = datetime.strptime(ultimo_record.get("data_inizio"), '%Y-%m-%d %H:%M:%S') if ultimo_record.get(
            "data_inizio") != '' else ultimo_record.get("data_inizio")
        end_json = datetime.strptime(ultimo_record.get("data_fine"), '%Y-%m-%d %H:%M:%S') if ultimo_record.get(
            "data_fine") != '' else ultimo_record.get("data_inizio")
        return start_json, end_json
    return start, end
