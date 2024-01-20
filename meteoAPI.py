import os
import pandas as pd
from openmeteo_requests import Client
import requests_cache
from retry_requests import retry

import openmeteo_requests


def historical_meteo_data(start_date_meteo, end_date_meteo):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 41.954706,
        "longitude": 12.486289,
        "start_date": start_date_meteo,
        "end_date": end_date_meteo,
        "hourly": ["temperature_2m", "rain", "cloud_cover"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_rain = hourly.Variables(1).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ),
        "temperature_2m": hourly_temperature_2m,
        "rain": hourly_rain,
        "cloud_cover": hourly_cloud_cover
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe




