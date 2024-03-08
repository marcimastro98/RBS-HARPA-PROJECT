import numpy as np
import pandas as pd
import psycopg2
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor
from matplotlib import pyplot as plt
from meteocalc import heat_index, dew_point, wind_chill
from sklearn.model_selection import train_test_split, cross_val_score, RandomizedSearchCV
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, BaggingRegressor, ExtraTreesRegressor, \
    AdaBoostRegressor, StackingRegressor, VotingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor

db_params = {
    'host': 'localhost',
    'database': 'HARPA',
    'user': 'user',
    'password': 'password',
    'port': '5432',
}

select_query = 'SELECT * FROM harpa.aggregazione_fascia_oraria WHERE 1=1'


def fetch_data_to_dataframe(query, connection_params):
    # Connessione al database
    connection = psycopg2.connect(**connection_params)
    # Esecuzione della query e caricamento dei dati nel DataFrame
    data_frame = pd.read_sql_query(query, connection)

    # Chiusura della connessione
    connection.close()

    return data_frame


def prepare_data():
    df = fetch_data_to_dataframe(select_query, db_params).dropna(axis=0, how='any')
    df['data'] = pd.to_datetime(df['data'])
    df_encoded = pd.get_dummies(df, columns=['is_smartworking'], prefix='smartworking', drop_first=True)
    df_encoded['month'] = df_encoded['data'].dt.month

    # Ottieni solo la parte intera della colonna "giorno settimana"
    df_encoded['giorno_settimana'] = df_encoded['giorno_settimana'].astype(int)

    # Ordina il DataFrame in base alla colonna 'data'
    df_encoded = df_encoded.sort_values(by=['data'])

    # Definisci le feature e la variabile target
    features = df_encoded.drop(
        columns=['id', 'kilowatt_edificio', 'kilowatt_fotovoltaico', 'kilowatt_ufficio', 'kilowatt_data_center',
                 'smartworking_NA', 'smartworking_OK'])  # Assicurati di rimuovere tutte le colonne non necessarie
    target = df_encoded['kilowatt_edificio']

    return features, target
