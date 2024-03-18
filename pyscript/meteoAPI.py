from datetime import datetime, timedelta
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def meteo_data_forecast(start_date_meteo, end_date_meteo, future, start_future_meteo_data):
    if start_date_meteo is not None and end_date_meteo is not None:
        start_date_meteo = start_date_meteo.strftime('%Y-%m-%d')
        end_date_meteo = end_date_meteo.strftime('%Y-%m-%d')

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    url_future = "https://api.open-meteo.com/v1/forecast"
    if future:
        start_future_meteo_data = start_future_meteo_data.strftime('%Y-%m-%d')
        end_date = (datetime.today() + timedelta(days=14)).strftime('%Y-%m-%d')
    else:
        end_date = end_date_meteo
    params = {
        "latitude": 41.954706,
        "longitude": 12.486289,
        "start_date": start_future_meteo_data if future else start_date_meteo,
        "end_date": end_date,
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation",
                   "rain", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover",
                   "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "wind_speed_10m", "wind_direction_10m", 
                   "is_day", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance", "global_tilted_irradiance", "terrestrial_radiation"],
    }
    responses = openmeteo.weather_api(url_future if future else url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_data = {}
    for i, variable in enumerate(
            ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain",
             "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover",
             "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "wind_speed_10m", "wind_direction_10m", 
             "is_day", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance", "global_tilted_irradiance", "terrestrial_radiation"]):
        hourly_data[variable] = hourly.Variables(i).ValuesAsNumpy()

    hourly_data["date"] = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe
