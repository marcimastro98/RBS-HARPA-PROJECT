import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd


def historical_meteo_data(start_date, end_date):
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 41.954706,
        "longitude": 12.486289,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "sunrise", "sunset",
                  "daylight_duration", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours"],
        "timezone": "auto"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(2).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(5).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(6).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(7).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(8).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(9).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s"),
        end=pd.to_datetime(daily.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )[1:],
        "temperature_2m_max": daily_temperature_2m_max[1:],
        "temperature_2m_min": daily_temperature_2m_min[1:],
        "temperature_2m_mean": daily_temperature_2m_mean[1:],
        "daylight_duration": daily_daylight_duration[1:],
        "precipitation_sum": daily_precipitation_sum[1:],
        "rain_sum": daily_rain_sum[1:],
        "snowfall_sum": daily_snowfall_sum[1:],
        "precipitation_hours": daily_precipitation_hours[1:]
    }

    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe = daily_dataframe.reset_index()
    daily_dataframe.rename(columns={'index': 'ID'}, inplace=True)
    daily_dataframe.to_csv('dataset_result/meteo/historical_meteo.csv', index=True)
    return daily_dataframe
