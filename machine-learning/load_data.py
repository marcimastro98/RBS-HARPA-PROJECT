import numpy as np
import pandas as pd
from meteocalc import heat_index, wind_chill
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine


def fetch_data_to_dataframe():
    # Utilizza SQLAlchemy per creare una connessione
    engine = create_engine('postgresql://user:password@localhost:5432/HARPA')
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

    # Applicazione della funzione di calcolo solo dove ha senso fisicamente
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
        # df['hour'] = df['data'].dt.hour
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
                 'kilowatt_fotovoltaico'])  # Assicurati di rimuovere tutte le colonne non necessarie
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
