import meteo_table
from pyscript.neural_network import calculate_future_consumption


if __name__ == '__main__':
    #conn = meteo_table.db_conn()
    #meteo_table.update_tables_meteo_data(conn[0], conn[1], conn[2])
    calculate_future_consumption()
