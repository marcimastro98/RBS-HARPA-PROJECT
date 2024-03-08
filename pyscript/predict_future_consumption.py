from datetime import datetime
import pandas as pd
from joblib import load
from insert_update import assign_time_slot
from meteoAPI import meteo_data_forecast

if __name__ == '__main__':
    meteo_data_future_forecast = meteo_data_forecast(None, None, True, datetime.today())

    meteo_data_future_forecast['date'] = pd.to_datetime(meteo_data_future_forecast['date'])

    meteo_data_future_forecast['day'] = meteo_data_future_forecast['date'].dt.day
    meteo_data_future_forecast['month'] = meteo_data_future_forecast['date'].dt.month
    meteo_data_future_forecast['year'] = meteo_data_future_forecast['date'].dt.year
    meteo_data_future_forecast['data'] = meteo_data_future_forecast['date'].dt.strftime('%d/%m/%Y')

    meteo_data_future_forecast['fascia_oraria'] = meteo_data_future_forecast['date'].dt.hour.apply(assign_time_slot)
    meteo_data_future_forecast['giorno_settimana'] = meteo_data_future_forecast['date'].dt.dayofweek

    ensemble = load('../machine-learning/model/ensemble_model.joblib')

    correct_column_order = [
        'fascia_oraria', 'giorno_settimana', 'rain', 'cloud_cover',
        'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m',
        'temperature_2m', 'dew_point_2m', 'apparent_temperature',
        'precipitation', 'snowfall', 'snow_depth', 'weather_code',
        'pressure_msl', 'surface_pressure', 'cloud_cover_low',
        'cloud_cover_mid', 'cloud_cover_high', 'month', 'day', 'year'
    ]
    X_to_predict = meteo_data_future_forecast[correct_column_order]

    predictions = ensemble.predict(X_to_predict)
    meteo_data_future_forecast['previsioni'] = predictions

    # Raggruppa per data, fascia oraria e giorno della settimana, e calcola la media delle previsioni
    grouped = meteo_data_future_forecast.groupby(['data', 'fascia_oraria', 'giorno_settimana'])['previsioni'].sum().reset_index()

    # Salva i risultati raggruppati in un CSV
    grouped.to_csv('../machine-learning/result/future_predictions_grouped.csv', index=False)
