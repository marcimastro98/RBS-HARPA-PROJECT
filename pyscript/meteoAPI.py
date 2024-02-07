from datetime import datetime, timedelta
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def meteo_data_forecast(start_date_meteo, end_date_meteo, future, start_future_meteo_data):
    start_date_meteo = start_date_meteo.strftime('%Y-%m-%d')
    end_date_meteo = end_date_meteo.strftime('%Y-%m-%d')

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    url2 = "https://api.open-meteo.com/v1/forecast"
    if future:
        start_future_meteo_data = start_future_meteo_data.strftime('%Y-%m-%d')
        end_date = (datetime.today() + timedelta(days=14)).strftime('%Y-%m-%d')
    else:
        end_date = end_date_meteo
    params = {
        "latitude": 41.954706,
        "longitude": 12.486289,
        "start_date": start_future_meteo_data if future is True else start_date_meteo,
        "end_date": end_date,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "cloud_cover", "wind_speed_10m", "wind_direction_10m"]
    }
    responses = openmeteo.weather_api(url2 if future else url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(3).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(5).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "relative_humidity_2m": hourly_relative_humidity_2m,
        "rain": hourly_rain, "cloud_cover": hourly_cloud_cover, "wind_speed_10m": hourly_wind_speed_10m,
        "wind_direction_10m": hourly_wind_direction_10m}

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe
