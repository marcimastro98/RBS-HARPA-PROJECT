import os

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

from pyscript import meteo_table
from pyscript.meteoAPI import meteo_data_forecast


def calculate_future_consumption():
    df = take_data()
    df = df.fillna(0)
    start_future_meteo_data = df.iloc[-1]['giorno'].strftime("%Y-%m-%d")
    meteo_data = meteo_data_forecast(None, None,
                                     True, start_future_meteo_data)
    # One-Hot Encoding
    df = pd.get_dummies(df, columns=['giorno_settimana', 'fascia_oraria', 'is_smartworking'])

    # Preparazione delle caratteristiche e dei valori target
    X = df.drop(['giorno', 'kilowatt_edificio_diff', 'kilowatt_data_center_diff', 'kilowatt_fotovoltaico_diff',
                 'kilowatt_ufficio_diff'], axis=1)
    y = df[
        ['kilowatt_edificio_diff', 'kilowatt_data_center_diff', 'kilowatt_fotovoltaico_diff', 'kilowatt_ufficio_diff']]

    # Divisione dei dati
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Grid Search con Cross-Validation
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10]
    }
    grid_search = GridSearchCV(estimator=RandomForestRegressor(random_state=42), param_grid=param_grid, cv=5,
                               scoring='neg_mean_squared_error', verbose=2, n_jobs=-1)
    grid_search.fit(X_train, y_train)
    print("Best parameters:", grid_search.best_params_)
    best_model = grid_search.best_estimator_

    # Valutazione del modello
    predictions = best_model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    print("Mean Squared Error with Grid Search:", mse)

    # Cross-Validation con il modello ottimizzato
    cv_scores = cross_val_score(estimator=best_model, X=X_train, y=y_train, cv=5, scoring='neg_mean_squared_error')
    mean_cv_scores = -1 * cv_scores.mean()
    std_cv_scores = cv_scores.std()
    print("Mean CV MSE:", mean_cv_scores)
    print("Std CV MSE:", std_cv_scores)


def take_data():
    # Risalire di una cartella rispetto alla directory corrente dello script
    base_dir = os.path.dirname(os.path.dirname(__file__))
    # Combinare il percorso base con la cartella 'env' e il nome del file '.env'
    env_path = os.path.join(base_dir, 'env', '.env')
    load_dotenv(env_path)
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = os.getenv('DB_PORT')
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    query = """
                SELECT giorno, giorno_settimana, fascia_oraria, kilowatt_edificio_diff, kilowatt_fotovoltaico_diff, 
                kilowatt_data_center_diff, kilowatt_ufficio_diff, temperature, rain, cloud_cover,relative_humidity_2m, 
                wind_speed_10m, wind_direction_10m, is_smartworking
                FROM HARPA.aggregazione_fascia_oraria ORDER BY giorno
            """
    data_frame = pd.read_sql(query, engine)
    return data_frame
