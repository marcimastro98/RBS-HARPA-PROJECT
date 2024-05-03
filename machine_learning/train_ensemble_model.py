from load_data import prepare_data
import os
import shutil
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, StackingRegressor
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.model_selection import train_test_split, GridSearchCV
from lightgbm import LGBMRegressor
from joblib import dump


def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


# def plot_feature_importances(model, feature_names):
#     if hasattr(model, 'feature_importances_'):
#         plt.figure(figsize=(12, 8))
#         importances = model.feature_importances_
#         indices = np.argsort(importances)
#         plt.title(f'Feature Importances - {model.__class__.__name__}')
#         plt.barh(range(len(indices)), importances[indices], color='b', align='center')
#         plt.yticks(range(len(indices)), [feature_names[i] for i in indices])
#         plt.xlabel('Relative Importance')
#         plt.show()


# def analyze_errors(y_true, y_pred, name):
#     errors = y_pred - y_true
#     plt.figure(figsize=(14, 7))
#     plt.hist(errors, bins=50, color='red', alpha=0.7)
#     plt.xlabel('Errore di predizione')
#     plt.ylabel('Frequenza')
#     plt.title(f'Distribuzione degli errori di predizione - {name}')
#     plt.show()


# def plot_predictions(y_true, y_pred, title):
#     plt.figure(figsize=(10, 6))
#     plt.scatter(y_true, y_pred, alpha=0.5, color='blue', label='Previsioni')
#     plt.title(title)
#     plt.xlabel('Valori Reali')
#     plt.ylabel('Previsioni')
#     max_val = max(y_true.max(), y_pred.max())
#     min_val = min(y_true.min(), y_pred.min())
#     plt.plot([min_val, max_val], [min_val, max_val], color='red', lw=2, linestyle='--', label='Linea di Identità')
#     plt.legend()
#     plt.show()


def optimize_hyperparameters(model, param_grid, X_train, Y_train):
    grid_search = GridSearchCV(model, param_grid, cv=3, scoring='neg_mean_squared_error', n_jobs=-1, verbose=1)
    grid_search.fit(X_train, Y_train)
    print(f"Best parameters for {model.__class__.__name__}: {grid_search.best_params_}")
    return grid_search.best_estimator_


# def check_target_distribution(y):
#     plt.figure(figsize=(10, 6))
#     sns.histplot(y, kde=True)
#     plt.title('Distribuzione Target (kilowatt_edificio)')
#     plt.xlabel('kilowatt_edificio')
#     plt.ylabel('Frequenza')
#     plt.show()


# Funzione per addestrare e valutare i modelli
def train_and_evaluate(X, y):
    X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.3, random_state=70)

    rf_param_grid = {
        'n_estimators': [70, 80, 100],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5, 10]
    }
    lgbm_param_grid = {
        'n_estimators': [70, 80, 100],
        'num_leaves': [31, 50, 100],
        'boosting_type': ['gbdt', 'dart']
    }

    print("Optimizing RandomForest...")
    rf_best = optimize_hyperparameters(RandomForestRegressor(), rf_param_grid, X_train, Y_train)

    print("Optimizing LGBM...")
    lgbm_best = optimize_hyperparameters(LGBMRegressor(), lgbm_param_grid, X_train, Y_train)

    estimators = [('rf', rf_best), ('lgbm', lgbm_best)]

    ensemble_stacking = StackingRegressor(estimators=estimators, final_estimator=Lasso(), cv=5)

    for estimator_name, model in estimators:
        print(f"Training {estimator_name}...")
        model.fit(X_train, Y_train)
        predictions = model.predict(X_test)

        # Calcolo delle metriche
        rmse = calculate_rmse(Y_test, predictions)
        mae = mean_absolute_error(Y_test, predictions)
        r2 = r2_score(Y_test, predictions)
        print(f"{estimator_name} model: RMSE: {rmse}, MAE: {mae}, R2: {r2}")

        # Visualizzazione dei grafici per ciascun modello
        # plot_predictions(Y_test, predictions, f'Previsioni vs Valori Reali - {estimator_name}')
        # if hasattr(model, 'feature_importances_'):
        #     plot_feature_importances(model, X_train.columns.tolist())
        # analyze_errors(Y_test, predictions, estimator_name)

    print("Training ensemble model...")
    ensemble_stacking.fit(X_train, Y_train)
    ensemble_preds = ensemble_stacking.predict(X_test)

    ensemble_rmse = np.sqrt(mean_squared_error(Y_test, ensemble_preds))
    ensemble_mae = mean_absolute_error(Y_test, ensemble_preds)
    ensemble_r2 = r2_score(Y_test, ensemble_preds)
    print(f"Ensemble model: RMSE: {ensemble_rmse}, MAE: {ensemble_mae}, R2: {ensemble_r2}")
    return ensemble_stacking


def train_and_save():
    X, Y = prepare_data()
    # check_target_distribution(Y)
    ensemble = train_and_evaluate(X, Y)
    model_folder = './model/'

    if os.path.exists(model_folder):
        shutil.rmtree(model_folder)

    os.makedirs(model_folder)
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{model_folder}/{formatted_datetime}-ensemble_model.joblib"
    print(f"---SALVATAGGIO ${file_name}---")
    dump(ensemble, file_name)