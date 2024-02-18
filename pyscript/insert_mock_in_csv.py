from datetime import datetime
import pandas as pd
import os


# Funzione per generare i dati
def generate_data(start, end, delta, kilowatt_start, kilowatt_increment):
    timestamps = pd.date_range(start=start, end=end, freq=delta)
    epoch_start = datetime.strptime('2023-12-01 00:00:00', '%Y-%m-%d %H:%M:%S').timestamp()
    kilowatts = kilowatt_start + ((timestamps.view(int) / 1e9 - epoch_start) / 300) * kilowatt_increment
    df = pd.DataFrame({'data': timestamps, 'kilowatt': kilowatts})
    return df


def append_data_to_csv(df, file_path):
    # Controlla se il file esiste per decidere se includere l'intestazione
    header = not os.path.isfile(file_path)
    df.to_csv(file_path, mode='a', index=False, header=header)


if __name__ == '__main__':
    # Parametri
    start_date = '2023-12-01 00:00:00'
    end_date = '2024-01-01 00:00:00'
    delta = '5T'  # 5 minuti
    kilowatt_start = 100000
    kilowatt_increment = 0.41

    # Percorsi dei file
    edificio_path = '../Dataset/Generale_Edificio_Energia_Attiva.csv'
    data_center_path = '../Dataset/Generale_Data_Center_Energia_Attiva.csv'
    fotovoltaico_path = '../Dataset/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.csv'

    # Genera i dati
    df_edificio = generate_data(start_date, end_date, delta, kilowatt_start, kilowatt_increment)
    df_data_center = generate_data(start_date, end_date, delta, kilowatt_start, kilowatt_increment)
    df_fotovoltaico = generate_data(start_date, end_date, delta, kilowatt_start, kilowatt_increment)

    # Appende i dati ai file CSV
    append_data_to_csv(df_edificio, edificio_path)
    append_data_to_csv(df_data_center, data_center_path)
    append_data_to_csv(df_fotovoltaico, fotovoltaico_path)

    print("Dati aggiunti ai file CSV.")
