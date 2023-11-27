from datetime import datetime

from calculate_consumption import calculate_consumption
from meteoAPI import historical_meteo_data
import pandas as pd


def create_csv_meteo_consumption():
    consumption = calculate_consumption()

    for name, data_dict in consumption:
        print(data_dict.columns)
        start_date_meteo = str(data_dict.index[0].date())  # Use .date() to get only the date part without time
        end_date_meteo = str(data_dict.index[-1].date())

        start_date_meteo = datetime.strptime(start_date_meteo, '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date_meteo = datetime.strptime(end_date_meteo, '%Y-%m-%d').strftime('%Y-%m-%d')

        meteo_data = historical_meteo_data(start_date_meteo, end_date_meteo)
        combined_df = pd.merge(consumption['daily'][name], meteo_data, on='ID', how='left')
        combined_df.to_csv(f'dataset_result/meteo_consumption/meteo_per_consumption_{str(name)}.csv', index=False)


if __name__ == '__main__':
    create_csv_meteo_consumption()
