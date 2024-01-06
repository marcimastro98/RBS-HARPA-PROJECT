import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from math import sqrt


def isSmartWorkingDay(consumption_per_slot_hour):
    print("da fare")