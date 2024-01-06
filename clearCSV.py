from datetime import datetime

import pandas as pd


def clearCSV(csv_files):
    dfs = {}
    # Ciclo per leggere ogni file CSV e caricarlo in un DataFrame
    for file in csv_files:
        # Il nome del DataFrame è derivato dal nome del file
        df_name = file.split('/')[-1].replace('.csv', '').replace(' ', '_').replace('-', '_')
        dfs[df_name] = pd.read_csv(file)
        dfs[df_name].drop_duplicates(inplace=True)
        dfs[df_name].rename(columns={"data": "Date", "valore": "Kilowatt"}, inplace=True)
        dfs[df_name]['Date'] = pd.to_datetime(dfs[df_name]['Date'], format='%Y-%m-%d %H:%M:%S')
        dfs[df_name].to_csv(file, index=False)
    return dfs


