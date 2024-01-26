import os
import time

import psycopg2
from dotenv import load_dotenv
from psycopg2._psycopg import OperationalError


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

            tables = cur.fetchall()
            print(f"Connection to {db_name} established, tables:{tables}")
            return [tables, cur, conn]
        except OperationalError as e:
            print(f"Errore: {e}"
                  f"Connessione fallita, nuovo tentativo in {10} secondi.")
            time.sleep(10)
