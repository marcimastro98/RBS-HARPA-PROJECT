import pandas as pd
from openmeteo_requests import Client
import requests_cache
from retry_requests import retry


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
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "pressure_msl",
                   "surface_pressure",
                   "cloud_cover"],
        "timezone": "Europe/Rome",
        "timezone_abbreviation": "CEST"
    }

    responses = openmeteo.weather_api(url, params=params)

    # Assuming that the response object has the required methods to access the weather data
    response = responses[0]

    # Process hourly data. Assuming that the hourly object has methods to get the data as NumPy arrays
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
    hourly_rain = hourly.Variables(3).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(4).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(5).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(6).ValuesAsNumpy()

    start = pd.to_datetime(hourly.Time(), unit="s").normalize()
    if start.day != 1:
        start += pd.Timedelta(days=1)
    # in ore
    hourly_data = {"date": pd.date_range(
        start=start,
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m[1:],
        "relative_humidity_2m": hourly_relative_humidity_2m[1:],
        "precipitation": hourly_precipitation[1:],
        "rain": hourly_rain[1:],
        "pressure_msl": hourly_pressure_msl[1:],
        "surface_pressure": hourly_surface_pressure[1:],
        "cloud_cover": hourly_cloud_cover[1:]
    }

    # Creating the DataFrame
    hourly_dataframe = pd.DataFrame(hourly_data)
    hourly_dataframe['date'] = pd.to_datetime(hourly_dataframe['date'])
    hourly_dataframe.set_index('date', inplace=True)
    daily_stats = hourly_dataframe.resample('D').agg({
        'temperature_2m': ['mean', 'min', 'max'],
        'relative_humidity_2m': ['mean', 'min', 'max'],
        'precipitation': ['mean', 'min', 'max'],
        'rain': ['mean', 'min', 'max'],
        'pressure_msl': ['mean', 'min', 'max'],
        'surface_pressure': ['mean', 'min', 'max'],
        'cloud_cover': ['mean', 'min', 'max'],
    }).round(1)
    daily_stats.columns = ['_'.join(col) for col in daily_stats.columns]
    daily_stats.reset_index(inplace=True)
    daily_stats = daily_stats.reset_index()
    daily_stats.rename(columns={'index': 'ID'}, inplace=True)
    daily_stats.to_csv('dataset_result/meteo/historical_meteo.csv', index=False)
    return daily_stats
