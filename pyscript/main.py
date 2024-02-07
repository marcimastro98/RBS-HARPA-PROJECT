from db_conn.db_conn import db_conn
from pyscript.insert_update import update_tables_meteo_data

if __name__ == '__main__':
    conn = db_conn()
    update_tables_meteo_data(conn[0], conn[1], conn[2])
