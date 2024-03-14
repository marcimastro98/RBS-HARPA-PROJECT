import glob
from datetime import datetime
import pandas as pd
from joblib import load
from meteocalc import heat_index, wind_chill
from sklearn.preprocessing import StandardScaler

from insert_update import assign_time_slot
from meteoAPI import meteo_data_forecast


def calculate_additional_weather_features(df):
    # Conversione della temperatura da Celsius a Fahrenheit per meteocalc
    df['temp_f'] = df['temperature_2m'] * 9 / 5 + 32
    df['wind_speed_mph'] = df['wind_speed_10m'] * 2.237  # conversione da m/s a mph

    # Applicazione della funzione di calcolo solo dove ha senso fisicamente
    df['heat_index'] = df.apply(
        lambda row: heat_index(temperature=row['temp_f'], humidity=row['relative_humidity_2m'])
        if row['temp_f'] >= 80 and row['relative_humidity_2m'] >= 40 else row['temp_f'], axis=1)

    df['wind_chill'] = df.apply(
        lambda row: wind_chill(temperature=row['temp_f'], wind_speed=row['wind_speed_mph'])
        if row['temp_f'] <= 50 and row['wind_speed_mph'] >= 3 else row['temp_f'], axis=1)

    # Rimozione delle colonne temporanee
    df.drop(columns=['temp_f', 'wind_speed_mph'], inplace=True)

    return df


if __name__ == '__main__':
    meteo_data_future_forecast = meteo_data_forecast(None, None, True, datetime.today())
    meteo_data_future_forecast = calculate_additional_weather_features(meteo_data_future_forecast)
    meteo_data_future_forecast['date'] = pd.to_datetime(meteo_data_future_forecast['date'])

    meteo_data_future_forecast['hour'] = meteo_data_future_forecast['date'].dt.hour
    meteo_data_future_forecast['day'] = meteo_data_future_forecast['date'].dt.day
    meteo_data_future_forecast['month'] = meteo_data_future_forecast['date'].dt.month
    meteo_data_future_forecast['year'] = meteo_data_future_forecast['date'].dt.year
    # meteo_data_future_forecast['data'] = meteo_data_future_forecast['date'].dt.strftime('%d/%m/%Y')

    meteo_data_future_forecast['fascia_oraria'] = meteo_data_future_forecast['date'].dt.hour.apply(assign_time_slot)
    meteo_data_future_forecast['giorno_settimana'] = meteo_data_future_forecast['date'].dt.dayofweek

    # meteo_data_future_forecast['fascia_oraria'] = meteo_data_future_forecast['date'].dt.hour.apply(assign_time_slot)
    # meteo_data_future_forecast['giorno_settimana'] = meteo_data_future_forecast['date'].dt.dayofweek

    model_files = glob.glob('../machine-learning/model/*ensemble_model.joblib')
    ensemble = load(model_files[0])

    correct_column_order = ['rain', 'cloud_cover', 'fascia_oraria',
                            'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m',
                            'temperature_2m', 'dew_point_2m', 'apparent_temperature',
                            'precipitation', 'weather_code',
                            'pressure_msl', 'surface_pressure', 'cloud_cover_low',
                            'cloud_cover_mid', 'cloud_cover_high', 'heat_index', 'wind_chill', 'year', 'month', 'day',
                            'hour',
                            'giorno_settimana']
    X_to_predict = meteo_data_future_forecast[correct_column_order].copy()

    features_to_scale = ['rain', 'cloud_cover',
                         'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m',
                         'temperature_2m', 'dew_point_2m', 'apparent_temperature',
                         'precipitation', 'weather_code',
                         'pressure_msl', 'surface_pressure', 'cloud_cover_low',
                         'cloud_cover_mid', 'cloud_cover_high', 'heat_index', 'wind_chill']
    scaler = StandardScaler()
    X_to_predict[features_to_scale] = scaler.fit_transform(X_to_predict[features_to_scale])
    # X_to_predict = X_to_predict.groupby(['year', 'month', 'day', 'fascia_oraria', 'giorno_settimana'])
    columns_to_exclude = ['year', 'month', 'day', 'fascia_oraria', 'giorno_settimana']

    predictions = ensemble.predict(X_to_predict)
    # Inizializza il dataframe con le previsioni
    meteo_data_future_forecast['previsioni'] = predictions

    # Crea un elenco di colonne da sommare escludendo le specifiche colonne
    columns_to_sum = [col for col in meteo_data_future_forecast.columns
                      if meteo_data_future_forecast[col].dtype in ['int64', 'float64']
                      and col not in columns_to_exclude]

    # Effettua il raggruppamento e la somma solo delle colonne numeriche
    meteo_data_future_forecast_aggregated = meteo_data_future_forecast.groupby(
        ['year', 'month', 'day', 'fascia_oraria', 'giorno_settimana'], as_index=False
    )[columns_to_sum].sum()

    # Aggiungi le colonne non numeriche al dataframe aggregato dopo la somma
    for col in columns_to_exclude:
        meteo_data_future_forecast_aggregated[col] = meteo_data_future_forecast.groupby(
            ['year', 'month', 'day', 'fascia_oraria', 'giorno_settimana']
        )[col].first().values

    # Riordina le colonne come desiderato
    meteo_data_future_forecast_aggregated = meteo_data_future_forecast_aggregated[
        ['year', 'month', 'day', 'fascia_oraria', 'giorno_settimana'] + columns_to_sum
        ]

    # Salva i risultati raggruppati in un CSV
    meteo_data_future_forecast_aggregated.to_csv(
        f'../machine-learning/result/{datetime.now().strftime("%Y%m%d%H%M%S")}-future_predictions_grouped.csv',
        index=False
    )

