import meteo_table
from db_conn.db_conn import db_conn

if __name__ == '__main__':
    conn = db_conn()
    meteo_table.update_tables_meteo_data(conn[0], conn[1], conn[2])
