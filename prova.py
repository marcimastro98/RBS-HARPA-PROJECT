import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

if __name__ == '__main__':
	import openmeteo_requests

	import requests_cache
	import pandas as pd
	from retry_requests import retry

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
		"start_date": "2021-01-01",
		"end_date": "2023-11-11",
		"daily": "temperature_2m_max"
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

	daily_data = {"date": pd.date_range(
		start=pd.to_datetime(daily.Time(), unit="s"),
		end=pd.to_datetime(daily.TimeEnd(), unit="s"),
		freq=pd.Timedelta(seconds=daily.Interval()),
		inclusive="left"
	), "temperature_2m_max": daily_temperature_2m_max}

	daily_dataframe = pd.DataFrame(data=daily_data)
	print(daily_dataframe)

