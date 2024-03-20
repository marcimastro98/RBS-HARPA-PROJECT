import logging
import os
import shutil

log_directory = '../log/'
log_filename = 'meteo_update.log'
full_log_path = os.path.join(log_directory, log_filename)


def create_log():
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    else:
        shutil.rmtree(log_directory)
        os.makedirs(log_directory)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[logging.FileHandler(full_log_path), logging.StreamHandler()])
    logging.info(f"Creata la cartella dei log {log_directory}")
    return logging

