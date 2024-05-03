import os
import sys
from clean_csv import clean
from db_conn.db_conn import db_conn
from log_function import create_log
sys.path.append(os.path.abspath('../machine_learning'))
import machine_learning.train_ensemble_model as ml


if __name__ == '__main__':
    log = create_log()
    tables, cur, conn = db_conn(log)
    clean(log, cur, conn)
    ml.train_and_save()
