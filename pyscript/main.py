from clean_csv import clean
from db_conn.db_conn import db_conn
from log_function import create_log

if __name__ == '__main__':
    log = create_log()
    tables, cur, conn = db_conn(log)
    clean(log, cur, conn)
