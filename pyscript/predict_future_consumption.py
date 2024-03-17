import glob
import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from joblib import load
from meteocalc import heat_index, wind_chill
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine
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


def create_future_predictions_df():
    meteo_data_future_forecast = meteo_data_forecast(None, None, True, datetime.today())
    meteo_data_future_forecast = calculate_additional_weather_features(meteo_data_future_forecast)
    meteo_data_future_forecast['date'] = pd.to_datetime(meteo_data_future_forecast['date'])

    meteo_data_future_forecast['hour'] = meteo_data_future_forecast['date'].dt.hour
    meteo_data_future_forecast['day'] = meteo_data_future_forecast['date'].dt.day
    meteo_data_future_forecast['month'] = meteo_data_future_forecast['date'].dt.month
    meteo_data_future_forecast['year'] = meteo_data_future_forecast['date'].dt.year

    meteo_data_future_forecast['date_str'] = meteo_data_future_forecast['year'].astype(str) + '-' + \
                                             meteo_data_future_forecast['month'].astype(str) + '-' + \
                                             meteo_data_future_forecast['day'].astype(str)

    meteo_data_future_forecast['fascia_oraria'] = meteo_data_future_forecast['date'].dt.hour.apply(assign_time_slot)
    meteo_data_future_forecast['giorno_settimana'] = meteo_data_future_forecast['date'].dt.dayofweek

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

    predictions = ensemble.predict(X_to_predict)
    meteo_data_future_forecast['previsioni'] = predictions

    meteo_data_future_forecast.drop(columns=['date_str'], inplace=True)

    meteo_data_future_forecast_grouped = meteo_data_future_forecast.groupby(
        ['year', 'month', 'day', 'giorno_settimana', 'fascia_oraria']
    ).agg({
        'rain': 'mean',
        'cloud_cover': 'mean',
        'relative_humidity_2m': 'mean',
        'wind_speed_10m': 'mean',
        'wind_direction_10m': 'mean',
        'temperature_2m': 'mean',
        'dew_point_2m': 'mean',
        'apparent_temperature': 'mean',
        'precipitation': 'mean',
        'weather_code': 'mean',
        'pressure_msl': 'mean',
        'surface_pressure': 'mean',
        'cloud_cover_low': 'mean',
        'cloud_cover_mid': 'mean',
        'cloud_cover_high': 'mean',
        'heat_index': 'mean',
        'wind_chill': 'mean',
        'previsioni': 'sum'  # Calcola la media delle previsioni per ogni fascia oraria
    }).reset_index()

    meteo_data_future_forecast_grouped['date'] = pd.to_datetime(
        meteo_data_future_forecast_grouped[['year', 'month', 'day']])
    meteo_data_future_forecast_grouped.drop(['year', 'month', 'day'], axis=1, inplace=True)
    meteo_data_future_forecast_grouped['update_date'] = datetime.now()

    return meteo_data_future_forecast_grouped


def add_predictio_to_db():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    env_path = os.path.join(base_dir, 'env', '.env')

    load_dotenv(env_path)
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')

    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    HOST = db_host
    USER = db_user
    PASSWORD = db_password
    DATABASE = db_name
    SCHEMA = 'harpa'
    PORT = db_port

    # Stringa di connessione
    DATABASE_URL = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

    # Creazione dell'engine di SQLAlchemy
    engine = create_engine(DATABASE_URL, echo=False)


    df = create_future_predictions_df()
    df.rename(columns={'previsioni': 'kilowatt_edificio_prediction'}, inplace=True)

    # Creazione della tabella se non esiste e inserimento dei dati
    df.to_sql('future_predictions', con=engine, schema=SCHEMA, if_exists='append', index=False)

    print("Dati inseriti con successo.")


if __name__ == '__main__':
    add_predictio_to_db()
