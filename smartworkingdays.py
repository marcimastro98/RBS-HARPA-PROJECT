from datetime import datetime
import pandas as pd
from openmeteo_requests import Client
import requests_cache
from retry_requests import retry


def smartworking_dataset():
    df = merge_meteo_to_consumption()
    df['Giorno'] = pd.to_datetime(df['Giorno'], format='%d/%m/%y')
    mean_delta_ufficio = calculate_mean_delta_ufficio(df)
    bad_weather_codes = [95, 96, 99]
    high_precipitation_threshold = 10

    df['Smartworking_Status'] = df.apply(
        lambda row: 'Smartworking' if is_smartworking_day(row, mean_delta_ufficio, bad_weather_codes,
                                                          high_precipitation_threshold) else 'In Office',
        axis=1)
    df.to_csv('./dataset_sql_result_day/merged_data_meteo.csv', index=False)


def calculate_mean_delta_ufficio(df):
    conditions = (
            (df['NOME_GIORNO'].isin(['sabato', 'domenica'])) |
            (df['Giorno'].dt.month == 8) |
            ((df['Giorno'].dt.month == 12) & (df['Giorno'].dt.day >= 24)) |
            ((df['Giorno'].dt.month == 1) & (df['Giorno'].dt.day <= 9))
    )
    filtered_df = df[conditions & (df['DELTA_UFFICIO'] >= 2)]
    return filtered_df['DELTA_UFFICIO'].mean()


def is_smartworking_day(row, mean_delta_ufficio, bad_weather_codes, high_precipitation_threshold):
    avg_temp = (row['temperature_2m_max'] + row['temperature_2m_min']) / 2
    return (
            row['CHECK_DAY'] == 'WorkDays' and
            row['DELTA_UFFICIO'] < 100 and
            (row['weather_code'] in bad_weather_codes or
             row['precipitation_sum(mm)'] > high_precipitation_threshold or
             avg_temp <= 5)
    )


def merge_meteo_to_consumption():
    df = pd.read_csv('./dataset_sql_result_day/Excel_estrazione.csv', delimiter=';')
    start_date_meteo = datetime.strptime(df['Giorno'][0], '%d/%m/%y').strftime('%Y-%m-%d')
    end_date_meteo = datetime.strptime(df['Giorno'].iloc[-1], '%d/%m/%y').strftime('%Y-%m-%d')
    meteo_data = historical_meteo_data(start_date_meteo, end_date_meteo)
    # Convertire il formato della data
    meteo_data['date'] = meteo_data['date'].apply(lambda d: d.strftime('%d/%m/%y'))
    df_merged = df.merge(meteo_data, left_on='Giorno', right_on='date')
    df_merged.to_csv('./dataset_sql_result_day/merged_data_meteo.csv', index=False)
    return df_merged


def historical_meteo_data(start_date_meteo, end_date_meteo):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = Client(session=retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"
    # Make sure all required weather variables are listed here
    params = {
        "latitude": 41.954706,
        "longitude": 12.486289,
        "start_date": start_date_meteo,
        "end_date": end_date_meteo,
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_sum", "rain_sum",
                  "precipitation_probability_max"],
        "timezone": "Europe/Rome",
        "timezone_abbreviation": "CEST"
    }

    responses = openmeteo.weather_api(url, params=params)

    # Assuming that the response object has the required methods to access the weather data
    response = responses[0]

    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(4).ValuesAsNumpy()
    # daily_precipitation_probability_max = daily.Variables(5).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s"),
        end=pd.to_datetime(daily.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    ), "weather_code": daily_weather_code, "temperature_2m_max": daily_temperature_2m_max,
        "temperature_2m_min": daily_temperature_2m_min, "precipitation_sum(mm)": daily_precipitation_sum,
        "rain_sum(mm)": daily_rain_sum}
    daily_dataframe = pd.DataFrame(data=daily_data)
    return daily_dataframe
