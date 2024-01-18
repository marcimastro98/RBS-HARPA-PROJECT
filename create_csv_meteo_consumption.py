import os
from datetime import datetime
from calculate_consumption import calculate_consumption
from meteoAPI import historical_meteo_data
import pandas as pd


def create_csv_meteo_consumption():
    consumption = calculate_consumption()
    last_day_in_dataset = None
    for name in consumption:
        for values in consumption[name]:
            df = pd.DataFrame.from_dict(consumption[name][values])
            start_date_meteo_str = ''
            end_date_meteo_str = ''
            if values == 'H':
                start_date_meteo_str = f'{df["Date"].iloc[0].split(" ")[0]}'
                end_date_meteo_str = df['Date'].iloc[-1].split(' ')[0]
                last_day_in_dataset = end_date_meteo_str
            elif values == 'D':
                start_date_meteo_str = f'{df["Date"].iloc[0].split(" ")[0]}'
                end_date_meteo_str = df['Date'].iloc[-1].split(' ')[0]
                last_day_in_dataset = end_date_meteo_str
            elif values == 'M':
                start_date_meteo_str = f'{df["Date"].iloc[0].split(" ")[0]}-01'
                end_date_meteo_str = f'{df["Date"].iloc[-1].split(" ")[0]}-01'
            elif values == 'Y':
                start_date_meteo_str = f'{df["Date"].iloc[0].split(" ")[0]}-01-31'
                end_date_meteo_str = last_day_in_dataset
            start_date_meteo = datetime.strptime(start_date_meteo_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            end_date_meteo = datetime.strptime(end_date_meteo_str, '%Y-%m-%d').strftime('%Y-%m-%d')

            meteo_data = historical_meteo_data(start_date_meteo, end_date_meteo, f'{values}_{name}')
            combined_df = pd.merge(df, meteo_data, on='ID', how='left')
            folder_path = f'dataset_result/meteo_consumption/{values}/'
            os.makedirs(folder_path, exist_ok=True)
            combined_df.to_csv(os.path.join(folder_path, f'{values}_meteo_per_consumption_{name}.csv'), index=False)
