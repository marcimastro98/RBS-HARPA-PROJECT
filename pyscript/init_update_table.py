import psycopg2


def run_sql_commands(cur, commands):
    for command in commands:
        cur.execute(command)


def copy_from_csv(cur, table_name, csv_file_path, delimiter=','):
    sql_copy_command = f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER '{delimiter}')"
    with open(csv_file_path, 'r') as f:
        cur.copy_expert(sql_copy_command, f)


def init_update_table(cur, conn):
    # Impostazione della time zone
    run_sql_commands(cur, ["SET TIMEZONE='Europe/Rome';"])

    # Creazione dello schema e delle tabelle
    commands = [
        "CREATE SCHEMA IF NOT EXISTS HARPA;",
        """CREATE TABLE IF NOT EXISTS HARPA.temp_data_center (
            data TIMESTAMP NOT NULL,
            kilowatt NUMERIC(10, 2)
        );""",
        """
        CREATE TABLE IF NOT EXISTS HARPA.temp_edificio (
            data TIMESTAMP NOT NULL,
            kilowatt NUMERIC(10, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS HARPA.temp_fotovoltaico (
            data TIMESTAMP NOT NULL,
            kilowatt NUMERIC(10, 2)
        );
        """
    ]
    run_sql_commands(cur, commands)

    # Esecuzione dei comandi COPY (adattati per eseguire da Python)
    copy_from_csv(cur, "HARPA.temp_data_center", "../Dataset/Generale_Data_Center_Energia_Attiva_no_duplicates.csv")
    copy_from_csv(cur, "HARPA.temp_edificio", "../Dataset/Generale_Edificio_Energia_Attiva_no_duplicates.csv")
    copy_from_csv(cur, "HARPA.temp_fotovoltaico", "../Dataset/Impianto_Fotovoltaico_Energia_Attiva_Prodotta_no_duplicates.csv")

    # Commit e chiusura
    conn.commit()
