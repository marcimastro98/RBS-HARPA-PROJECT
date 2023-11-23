import reimport pandas as pdfrom datetime import datetime, timedeltaimport openmeteo_requestsimport requests_cachefrom retry_requests import retrycsv_files = [    'Dataset/Generale_Data Center_Energia_Attiva.csv',    'Dataset/Generale_Edificio_Energia_Attiva.csv',    'Dataset/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.csv']dfs = {}# Ciclo per leggere ogni file CSV e caricarlo in un DataFramefor file in csv_files:    # Il nome del DataFrame è derivato dal nome del file    df_name = file.split('/')[-1].replace('.csv', '').replace(' ', '_').replace('-', '_')    dfs[df_name] = pd.read_csv(file)def calculate_consumption():    daily_consumption_dict = {}    monthly_consumption_dict = {}    consumption_dict = {}    for dataset in dfs:        consumption_per_date = to_date(dfs[dataset])        daily_consumption_dict[dataset] = daily_consumption(consumption_per_date, dataset)        monthly_consumption_dict[dataset] = monthly_consumption(consumption_per_date, dataset)        consumption_dict["daily"] = daily_consumption_dict        consumption_dict["monthly"] = monthly_consumption_dict    return consumption_dictdef monthly_consumption(consumption_per_date, name):    past_month = list(consumption_per_date)[0]    first_month = list(consumption_per_date)[0]    first_value_month = consumption_per_date[first_month]    last_value_month = 0    month_consumption = {}    for date_in_dict in consumption_per_date:        if past_month.date().month == date_in_dict.date().month:            last_value_month = consumption_per_date[date_in_dict]        elif past_month.date().month != date_in_dict.date().month:            if first_month is not None and past_month is not None:                float_result = float(last_value_month) - float(first_value_month)                result = f"{float_result:.2f}"                month_consumption[f"{first_month} -> {date_in_dict - timedelta(minutes=5)}"] = result            first_month = date_in_dict            first_value_month = consumption_per_date[date_in_dict]        past_month = date_in_dict    month_consumption = {k.strip(): v.strip() for k, v in month_consumption.items()}    sorted_month_consumption = dict(sorted(month_consumption.items()))    month_dataframe = pd.DataFrame(list(sorted_month_consumption.items()), columns=['Date', 'Kilowatt'])    month_dataframe = month_dataframe.reset_index()    month_dataframe.rename(columns={'index': 'ID'}, inplace=True)    month_dataframe.to_csv(f'dataset_result/{name}_monthly_dataframe_consumption.csv', index=False)    return month_dataframedef daily_consumption(consumption_per_date, name):    past_day = list(consumption_per_date)[0]    first_day = list(consumption_per_date)[0]    first_value_day = consumption_per_date[first_day]    last_value_day = 0    day_consumption = {}    for date_in_dict in consumption_per_date:        if past_day.date().day == date_in_dict.date().day:            last_value_day = consumption_per_date[date_in_dict]        elif past_day.date().day != date_in_dict.date().day:            if first_day is not None and past_day is not None:                float_result = float(last_value_day) - float(first_value_day)                result = f"{float_result:.2f}"                day_consumption[f"{first_day} -> {date_in_dict - timedelta(minutes=5)}"] = result            first_day = date_in_dict            first_value_day = consumption_per_date[date_in_dict]        past_day = date_in_dict    day_consumption = {k.strip(): v.strip() for k, v in day_consumption.items()}    sorted_day_consumption = dict(sorted(day_consumption.items()))    day_dataframe = pd.DataFrame(list(sorted_day_consumption.items()), columns=['Date', 'Consumption'])    day_dataframe = day_dataframe.reset_index()    day_dataframe.rename(columns={'index': 'ID'}, inplace=True)    day_dataframe.to_csv(f'dataset_result/{name}_daily_dataframe_consumption.csv', index=False)    return day_dataframedef to_date(dataset):    date_list = []    consumption_per_date = {}    for data in dataset['data']:        date_list.append(datetime.strptime(str(data), '%Y-%m-%d %H:%M:%S'))    for i, value in enumerate(dataset['valore']):        consumption_per_date[date_list[i]] = value    return consumption_per_date