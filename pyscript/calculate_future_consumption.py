import os

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
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
    df = pd.get_dummies(df, columns=['giorno_settimana']) #serve per sostituire le stringhe con dati numerici
    df = pd.get_dummies(df, columns=['fascia_oraria'])
    df = pd.get_dummies(df, columns=['is_smartworking'])
    X = df.drop(['giorno', 'kilowatt_edificio_diff', 'kilowatt_data_center_diff', 'kilowatt_fotovoltaico_diff',
                 'kilowatt_ufficio_diff'], axis=1)  # Caratteristiche
    y = df[
        ['kilowatt_edificio_diff', 'kilowatt_data_center_diff', 'kilowatt_fotovoltaico_diff', 'kilowatt_ufficio_diff']]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Allenamento del modello
    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Previsioni sul set di test
    predictions = model.predict(X_test)

    # Calcola e stampa l'importanza delle feature
    importances = model.feature_importances_
    feature_names = X.columns
    feature_importances = pd.DataFrame(sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True),
                                       columns=['Feature', 'Importance'])
    print(feature_importances)

    # Valutazione del modello
    mse = mean_squared_error(y_test, predictions)
    print("Mean Squared Error:", mse)

    # Supponendo che tu stia lavorando con una singola colonna target, ad esempio 'kilowatt_edificio_diff'
    real_values = y_test['kilowatt_edificio_diff'].values
    predicted_values = predictions[:, 0]  # Assumi che predictions abbia la stessa forma di y_test

    # Crea il DataFrame per l'analisi degli errori
    df_error_analysis = pd.DataFrame({'Real': real_values, 'Predicted': predicted_values})
    df_error_analysis['Error'] = df_error_analysis['Real'] - df_error_analysis['Predicted']

    # Visualizza gli errori
    plt.scatter(df_error_analysis['Predicted'], df_error_analysis['Error'])
    plt.xlabel('Predicted Values')
    plt.ylabel('Prediction Error')
    plt.title('Error Analysis')
    plt.axhline(y=0, color='r', linestyle='-')
    plt.show()

    # Analizza gli errori più grandi
    print(df_error_analysis.sort_values(by='Error', ascending=False))


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
