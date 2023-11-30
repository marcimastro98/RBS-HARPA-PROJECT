import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from math import sqrt


def isSmartWorkingDay():
    # Load your data into a DataFrame
    data = pd.read_csv('path_to_your_data.csv', parse_dates=['Date'])

    # Preprocess the data
    # Assuming 'Date', 'Temperature', 'Precipitation', and 'H_Kilowatt_Consumed' are columns in your DataFrame
    data['day_of_week'] = data['Date'].dt.dayofweek
    data['hour_of_day'] = data['Date'].dt.hour

    # Feature Selection
    features = ['Temperature', 'Precipitation', 'day_of_week', 'hour_of_day']
    X = data[features]
    y = data['H_Kilowatt_Consumed']

    # Data Splitting
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Model Training
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Model Evaluation
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = sqrt(mse)

    print(f'Root Mean Squared Error: {rmse}')
