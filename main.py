from calculate_consumption import calculate_consumption
from meteoAPI import historical_meteo_data
import pandas as pd


def create_csv_meteo_consumption(start_date_meteo, end_date_meteo):
    consumption = calculate_consumption()
    meteo_data = historical_meteo_data(start_date_meteo, end_date_meteo)

    for name, data_dict in consumption['daily'].items():
        # Ensure consumption data has a 'Date' column of the correct dtype
        # consumption_df = pd.DataFrame.from_dict(data_dict[0], orient='index', columns=['Kilowatt'])
        # consumption_df['Date'] = pd.to_datetime(consumption_df.index)

        # Combine consumption data with weather data
        combined_df = pd.merge(consumption['daily'][name], meteo_data, on='ID', how='left')

        # Save the combined data to a CSV file
        combined_df.to_csv(f'dataset_result/meteo_consumption/meteo_per_consumption_{name}.csv', index=True)


if __name__ == '__main__':
    start_date_fotovoltaico = "2021-05-21"
    start_date = '2021-01-01'
    end_date = '2023-11-13'
    create_csv_meteo_consumption(start_date, end_date)
