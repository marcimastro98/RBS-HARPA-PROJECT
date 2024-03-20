import os
import time
import psycopg2
from dotenv import load_dotenv


def db_conn(logging):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Risalire di 2 cartelle rispetto alla directory corrente dello script
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    # Combinare il percorso base con la cartella 'env' e il nome del file '.env'
    env_path = os.path.join(base_dir, 'env', '.env')

    load_dotenv(env_path)
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')
    conn = None
    cur = None
    tables = None
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

        except Exception as e:
            logging.error(f"Errore: {e}"
                          f"Connessione fallita, nuovo tentativo in {10} secondi.")
            time.sleep(10)
        finally:
            logging.info(f"Connection to {db_name} established")
            return tables, cur, conn
