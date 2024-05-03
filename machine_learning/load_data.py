import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from meteocalc import heat_index, wind_chill
from sklearn.preprocessing import StandardScaler


def fetch_data_to_dataframe():
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
    PORT = db_port
    engine = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    query = 'SELECT * FROM harpa.aggregazione_ora WHERE 1=1'
    df = pd.read_sql_query(query, engine)
    return df


def encode_cyclical(df, col, max_val):
    df[col + '_sin'] = np.sin(2 * np.pi * df[col] / max_val)
    df[col + '_cos'] = np.cos(2 * np.pi * df[col] / max_val)
    return df


def calculate_additional_weather_features(df):
    # Conversione della temperatura da Celsius a Fahrenheit per meteocalc
    df['temp_f'] = df['temperature_2m'] * 9 / 5 + 32
    df['wind_speed_mph'] = df['wind_speed_10m'] * 2.237  # conversione da m/s a mph

    df['heat_index'] = df.apply(
        lambda row: heat_index(temperature=row['temp_f'], humidity=row['relative_humidity_2m'])
        if row['temp_f'] >= 80 and row['relative_humidity_2m'] >= 40 else row['temp_f'], axis=1)

    df['wind_chill'] = df.apply(
        lambda row: wind_chill(temperature=row['temp_f'], wind_speed=row['wind_speed_mph'])
        if row['temp_f'] <= 50 and row['wind_speed_mph'] >= 3 else row['temp_f'], axis=1)

    df.drop(columns=['temp_f', 'wind_speed_mph'], inplace=True)

    return df


def filter_outliers(data, column, threshold):
    upper_limit = data[column].quantile(threshold)
    lower_limit = data[column].quantile(1 - threshold)
    filtered_data = data[(data[column] <= upper_limit) & (data[column] >= lower_limit)]
    return filtered_data


def convert_dates(df):
    if 'data' in df.columns:
        # Scomposizione in componenti separate
        df['year'] = df['data'].dt.year
        df['month'] = df['data'].dt.month
        df['day'] = df['data'].dt.day
        df['hour'] = df['data'].dt.hour
        df['giorno_settimana'] = df['data'].dt.dayofweek

        # Rimozione della colonna datetime originale
        df = df.drop('data', axis=1)
        sorted_data = df.sort_values(by=['year', 'month', 'day', 'fascia_oraria'])
        df = sorted_data
    return df


def prepare_data():
    original_df = fetch_data_to_dataframe().dropna(axis=0, how='any')
    df = original_df.drop(columns=['id'])
    df['data'] = pd.to_datetime(df['data'])
    df = convert_dates(df)

    df = df[df['kilowatt_fotovoltaico'] != 0]
    df = calculate_additional_weather_features(df)

    # Feature scaling
    features_to_scale = ['rain', 'cloud_cover',
                         'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m',
                         'temperature_2m', 'dew_point_2m', 'apparent_temperature',
                         'precipitation', 'weather_code',
                         'pressure_msl', 'surface_pressure', 'cloud_cover_low',
                         'cloud_cover_mid', 'cloud_cover_high', 'heat_index', 'wind_chill']
    scaler = StandardScaler()
    df[features_to_scale] = scaler.fit_transform(df[features_to_scale])

    features = df.drop(
        columns=['kilowatt_edificio', 'kilowatt_ufficio', 'kilowatt_data_center',
                 'kilowatt_fotovoltaico'])
    target = df['kilowatt_edificio']
    correct_column_order = ['rain', 'cloud_cover', 'fascia_oraria',
                            'relative_humidity_2m', 'wind_speed_10m', 'wind_direction_10m',
                            'temperature_2m', 'dew_point_2m', 'apparent_temperature',
                            'precipitation', 'weather_code',
                            'pressure_msl', 'surface_pressure', 'cloud_cover_low',
                            'cloud_cover_mid', 'cloud_cover_high', 'heat_index', 'wind_chill', 'year', 'month', 'day',
                            'hour',
                            'giorno_settimana']
    features = features[correct_column_order]

    return features, target
