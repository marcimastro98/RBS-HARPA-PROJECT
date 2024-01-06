import pandas as pd


def calculate_consumption_test(consumption_per_date, name):
    # Convert 'Date' column to datetime objects and set as index
    consumption_per_date['Date'] = pd.to_datetime(consumption_per_date['Date'], format='%Y-%m-%d %H:%M:%S')
    consumption_per_date.set_index('Date', inplace=True)

    # Define a function to format the date range string
    def format_date_range(start, frequ):
        if frequ == 'H':
            end = start + pd.Timedelta(hours=1)
            return start.strftime('%Y-%m-%d %H:00 -> ') + end.strftime('%H:00')
        elif frequ == 'D':
            return start.strftime('%Y-%m-%d')
        elif frequ == 'M':
            return start.strftime('%Y-%m')
        elif frequ == 'Y':
            return start.strftime('%Y')

    # Initialize a dictionary to hold the different consumption DataFrames
    consumption_diffs = {}

    # Calculate differences and format date ranges for hourly, daily, monthly, yearly
    for freq in ['H', 'D', 'M', 'Y']:
        diff = consumption_per_date['Kilowatt'].resample(freq).last() - consumption_per_date['Kilowatt'].resample(
            freq).first()
        diff = diff.reset_index()
        diff['Date'] = diff['Date'].apply(lambda x: format_date_range(x, freq))
        diff.rename(columns={'Kilowatt': f'{freq}_Kilowatt_Consumed'}, inplace=True)
        diff.insert(0, 'ID', range(len(diff)))
        consumption_diffs[freq] = diff
        diff.to_csv(f'dataset_result/difference_consumption/{freq}/{name}_{freq.lower()}_difference_consumption.csv',
                    index=False)

    return consumption_diffs

# Example usage:
# Assuming `consumption_data` is a DataFrame with 'Date' and 'Kilowatt' columns
# consumption_data = pd.read_csv('path_to_your_csv.csv')
# results = calculate_consumption_test(consumption_data, 'example_name')
