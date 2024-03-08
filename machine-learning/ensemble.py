import numpy as np
from catboost import CatBoostRegressor
from joblib import dump, load
from lightgbm import LGBMRegressor
from matplotlib import pyplot as plt
from sklearn.ensemble import StackingRegressor, GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error, make_scorer
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV, TimeSeriesSplit, \
    ParameterSampler
from xgboost import XGBRegressor
from load_data import prepare_data


def calculate_rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


def plot_predictions(y_true, y_pred, title):
    plt.figure(figsize=(10, 6))
    plt.scatter(y_true, y_pred, alpha=0.5, color='blue', label='Previsioni')
    plt.title(title)
    plt.xlabel('Valori Reali')
    plt.ylabel('Previsioni')

    max_val = max(y_true.max(), y_pred.max())
    min_val = min(y_true.min(), y_pred.min())

    plt.plot([min_val, max_val], [min_val, max_val], color='red', lw=2, linestyle='--', label='Linea di Identità')

    plt.legend()
    plt.show()


def plot_feature_importances(ensemble_, feature_names):
    for name, model in ensemble_.named_estimators_.items():
        # Verifica se il modello ha l'attributo `feature_importances_`
        if hasattr(model, 'feature_importances_'):
            plt.figure(figsize=(12, 8))
            importances = model.feature_importances_
            indices = np.argsort(importances)
            plt.title(f'Feature Importances - {name}')
            plt.barh(range(len(indices)), importances[indices], color='b', align='center')
            plt.yticks(range(len(indices)), [feature_names[i] for i in indices])
            plt.xlabel('Relative Importance')
            plt.show()


def analyze_errors(y_true, y_pred, name):
    errors = y_pred - y_true
    plt.figure(figsize=(14, 7))
    plt.hist(errors, bins=50, color='red', alpha=0.7)
    plt.xlabel('Errore di predizione')
    plt.ylabel('Frequenza')
    plt.title(f'Distribuzione degli errori di predizione - {name}')
    plt.show()


def hyperparameter_tuning_or_fit(model, params, X_train, Y_train, use_hyperparameter_tuning=True):
    cv_strategy = TimeSeriesSplit(n_splits=5)

    if use_hyperparameter_tuning:
        # Calcola il numero di combinazioni uniche possibili
        param_list = list(ParameterSampler(params, n_iter=2))
        n_iter = min(5, len(param_list))

        # Assicurati che n_iter non sia maggiore del numero di combinazioni possibili
        random_search = RandomizedSearchCV(
            model, params, n_iter=n_iter, cv=cv_strategy, scoring='neg_mean_squared_error',
            random_state=70, n_jobs=-1
        )
        random_search.fit(X_train, Y_train)
        return random_search.best_estimator_
    else:
        model.set_params(**{k: v[0] if isinstance(v, list) else v for k, v in params.items()})
        model.fit(X_train, Y_train)
        return model


def convert_dates(X_data):
    X_data['day'] = X_data['data'].dt.day
    X_data['month'] = X_data['data'].dt.month
    X_data['year'] = X_data['data'].dt.year

    X_data = X_data.drop('data', axis=1)  # Rimuovi la colonna originale 'data'
    return X_data


def train_and_evaluate(X, y):
    X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.3, random_state=70)
    X_train = convert_dates(X_train)
    X_test = convert_dates(X_test)

    models_and_hyperparameters = {
        'xgb': (XGBRegressor(objective='reg:squarederror'), {'n_estimators': [70, 100]}, True),
        'lgbm': (LGBMRegressor(), {'n_estimators': [70, 100]}, True),
        'catboost': (CatBoostRegressor(verbose=0), {'iterations': [70, 100]}, True),
        'gb': (GradientBoostingRegressor(), {'n_estimators': [70, 100]}, False),
        'rf': (RandomForestRegressor(), {'n_estimators': [70, 100]}, False),
        'ridge': (Ridge(), {'alpha': [1.0]}, False),
    }

    trained_models = []

    for model_name, (model, params, use_hyperparameter_tuning) in models_and_hyperparameters.items():
        trained_model = hyperparameter_tuning_or_fit(model, params, X_train, Y_train, use_hyperparameter_tuning)
        trained_models.append((model_name, trained_model))

    ensemble_model = StackingRegressor(estimators=trained_models, final_estimator=RandomForestRegressor(random_state=100))
    ensemble_model.fit(X_train, Y_train)
    ensemble_predictions = ensemble_model.predict(X_test)

    ensemble_rmse = calculate_rmse(Y_test, ensemble_predictions)
    ensemble_mae = mean_absolute_error(Y_test, ensemble_predictions)
    ensemble_r2 = r2_score(Y_test, ensemble_predictions)

    print(f'Ensemble: RMSE: {ensemble_rmse}, MAE: {ensemble_mae}, R2: {ensemble_r2}')

    plot_predictions(Y_test, ensemble_predictions, 'Previsioni vs Valori Reali - Ensemble')

    analyze_errors(Y_test, ensemble_predictions, "ensemble")

    feature_names = X_train.columns.tolist()
    plot_feature_importances(ensemble_model, feature_names)
    return ensemble_model


if __name__ == '__main__':
    X, Y = prepare_data()
    ensemble = train_and_evaluate(X, Y)
    dump(ensemble, './model/ensemble_model.joblib')
