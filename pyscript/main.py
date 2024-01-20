import meteo_table

if __name__ == '__main__':
    conn = meteo_table.db_conn()
    meteo_table.update_tables_meteo_data(conn[0], conn[1], conn[2])
    # smartworkingdays.smartworking_dataset()
