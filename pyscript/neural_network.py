import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from keras import Input, Model
from keras.src.callbacks import EarlyStopping, ModelCheckpoint
from keras.src.layers import Flatten
from keras.src.saving.saving_api import load_model
from matplotlib import pyplot as plt
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Embedding, Concatenate

from pyscript.meteoAPI import meteo_data_forecast


def calculate_future_consumption():
    # Carica i dati
    df = take_data()
    df = df.fillna(0)
    # Numero di categorie uniche per ciascuna variabile categorica
    num_fascia_oraria = len(df['fascia_oraria'].unique())
    num_is_smartworking = len(df['is_smartworking'].unique())

    df['fascia_oraria'] = df['fascia_oraria'].astype('category').cat.codes
    df['is_smartworking'] = df['is_smartworking'].astype('category').cat.codes

    df['solo_ora'] = df['solo_ora'].apply(lambda x: x.hour)
    # Assicurati che la colonna 'giorno' sia di tipo datetime
    df['giorno'] = pd.to_datetime(df['giorno'])
    df['giorno_settimana'] = df['giorno'].dt.dayofweek
    # Ora puoi usare l'accessore .dt per estrarre il giorno dell'anno
    df['giorno'] = df['giorno'].dt.dayofyear

    X_fascia_oraria = df['fascia_oraria']
    X_is_smartworking = df['is_smartworking']

    # Ora rimuovi queste colonne dal DataFrame principale prima di eseguire train_test_split
    X = df.drop(['fascia_oraria', 'is_smartworking', 'ora',
                 'kilowatt_edificio_diff', 'kilowatt_data_center_diff',
                 'kilowatt_fotovoltaico_diff', 'kilowatt_ufficio_diff'], axis=1)

    # Preparazione dei valori target
    y = df[['kilowatt_edificio_diff', 'kilowatt_data_center_diff',
            'kilowatt_fotovoltaico_diff', 'kilowatt_ufficio_diff']]

    # Divisione dei dati in set di addestramento e di test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    X_train_fascia_oraria, X_test_fascia_oraria = train_test_split(X_fascia_oraria, test_size=0.2, random_state=42)
    X_train_is_smartworking, X_test_is_smartworking = train_test_split(X_is_smartworking, test_size=0.2,
                                                                       random_state=42)

    X_train_fascia_oraria = np.array(X_train_fascia_oraria)
    X_train_is_smartworking = np.array(X_train_is_smartworking)
    X_train = np.array(X_train)

    X_test_fascia_oraria = np.array(X_test_fascia_oraria)
    X_test_is_smartworking = np.array(X_test_is_smartworking)
    X_test = np.array(X_test)

    # Costruzione del modello
    embedding_size = 4  # Dimensione degli embedding (puoi sperimentare con diversi valori)

    # Input layers
    input_fascia_oraria = Input(shape=(1,))
    input_is_smartworking = Input(shape=(1,))
    other_features = Input(shape=(X_train.shape[1],))

    # Embedding layers
    embedding_fascia_oraria = Embedding(num_fascia_oraria, embedding_size)(input_fascia_oraria)
    embedding_is_smartworking = Embedding(num_is_smartworking, embedding_size)(input_is_smartworking)

    # Appiattire gli embedding
    flattened_fascia_oraria = Flatten()(embedding_fascia_oraria)
    flattened_is_smartworking = Flatten()(embedding_is_smartworking)

    # Concatenazione
    concatenated = Concatenate()(
        [flattened_fascia_oraria, flattened_is_smartworking, other_features])

    # Altri strati della rete
    hidden1 = Dense(128, activation='relu')(concatenated)
    dropout = Dropout(0.2)(hidden1)
    hidden2 = Dense(64, activation='relu')(dropout)
    output = Dense(1)(hidden2)  # Assumi una sola unità di output per la regressione

    model = Model(inputs=[input_fascia_oraria, input_is_smartworking, other_features],
                  outputs=output)

    model.compile(optimizer='adam',
                  loss='mean_squared_error',
                  metrics=['mae', 'mse'])
    # Definisci i callback
    early_stopping = EarlyStopping(monitor='val_loss', patience=10)
    model_checkpoint = ModelCheckpoint('best_model.h5', save_best_only=True)

    # Addestramento del modello
    history = model.fit([X_train_fascia_oraria, X_train_is_smartworking, X_train],
                        y_train, epochs=50, batch_size=32, validation_split=0.2, verbose=1,
                        callbacks=[early_stopping, model_checkpoint])
    plot_history(history)
    best_model = load_model('best_model.h5')
    predicted_values = best_model.predict([X_test_fascia_oraria, X_test_is_smartworking, X_test])
    # Valutazione del modello migliore
    test_loss, test_mae, test_mse = best_model.evaluate([X_test_fascia_oraria, X_test_is_smartworking, X_test], y_test)
    print(f"Test Loss: {test_loss}, Test MAE: {test_mae}, Test MSE: {test_mse}")


    # Converti y_test in un array NumPy
    y_test_array = y_test.to_numpy()
    # Calcola gli errori percentuali
    absolute_errors = abs(y_test_array - predicted_values)
    # Calcola gli errori percentuali con gestione degli zeri
    percentage_errors = np.where(y_test_array == 0, 0, absolute_errors / y_test_array)

    # Verifica quali previsioni sono accurate
    threshold = 0.10
    accurate_predictions = percentage_errors <= threshold

    # Calcola l'accuratezza
    accuracy = sum(accurate_predictions) / len(accurate_predictions)
    print(f"Accuracy: {accuracy * 100}%")
    for i, col in enumerate(y_test.columns):
        col_errors = percentage_errors[:, i]
        print(f"Accuracy for {col}: {(1 - np.mean(col_errors)) * 100}%")


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
                SELECT ora, solo_ora, giorno, giorno_settimana, fascia_oraria, kilowatt_edificio_diff, kilowatt_fotovoltaico_diff, 
                kilowatt_data_center_diff, kilowatt_ufficio_diff, temperature, rain, cloud_cover,relative_humidity_2m, 
                wind_speed_10m, wind_direction_10m, is_smartworking
                FROM HARPA.aggregazione_ora ORDER BY ora
            """
    data_frame = pd.read_sql(query, engine)
    return data_frame


def plot_history(history):
    plt.figure(figsize=(8, 4))
    plt.subplot(1, 2, 1)
    plt.plot(history.history['loss'], label='Train Loss')
    plt.plot(history.history['val_loss'], label='Validation Loss')
    plt.title('Loss Over Epochs')
    plt.legend()

    plt.subplot(1, 2, 2)
    plt.plot(history.history['mae'], label='Train MAE')
    plt.plot(history.history['val_mae'], label='Validation MAE')
    plt.title('MAE Over Epochs')
    plt.legend()
    plt.show()
