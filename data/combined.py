import pandas as pd

# Function to load and preprocess datasets
def load_and_preprocess(file_name, is_price_data=False):
    """Load and sort dataset by date."""
    try:
        # df = pd.read_csv(file_name)
        file_name = file_name.replace(" ", "%")
        url = f"https://raw.githubusercontent.com/ahnb24/AmirhessamNakhaei.github.io/main/data/{file_name}.csv"
        df = pd.read_csv(url)
        if is_price_data:  # Only preprocess the price data for GBP-IRR ratio
            df['GBP-IRR ratio'] = df['GBP-IRR ratio'].str.replace(',', '').astype(float)
        df = df.sort_values(by='date(shamsi)')
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return None

# Function to sync GBP-IRR ratio data with a dataset
def sync_gbp_irr_ratio(price_df, dataset):
    """Sync the GBP-IRR ratio with the dataset based on the date."""
    try:
        dataset = dataset.copy()  # Avoid SettingWithCopyWarning
        dataset['GBP-IRR ratio'] = dataset['date(shamsi)'].map(
            lambda d: price_df.loc[price_df['date(shamsi)'] == d, 'GBP-IRR ratio'].iloc[0]
            if d in price_df['date(shamsi)'].values else None
        )
        return dataset.dropna(subset=['GBP-IRR ratio'])  # Drop rows with missing ratio
    except Exception as e:
        print(f"Error syncing GBP-IRR ratio: {e}")
        return dataset

# Function to calculate trade value
def calculate_trade_value(dataset):
    """Calculate the total trade value for the dataset."""
    try:
        dataset = dataset.copy()  # Avoid SettingWithCopyWarning
        dataset.loc[:, 'sum_day'] = (
            (dataset['number of buy'] * dataset['avg price buy'] +
             dataset['number of sell'] * dataset['avg price sell']) / dataset['GBP-IRR ratio']
        )
        return dataset['sum_day'].sum()
    except Exception as e:
        print(f"Error calculating trade value: {e}")
        return 0

# Load GBP-IRR ratio data
price_data = load_and_preprocess('GBP-IRR.csv', is_price_data=True)

# List of datasets to process
names = [
    'Amirali Nakhaei', 'Amirhessam Nakhaei', 'Ahmad Nakhaei',
    'Fatemeh Komlakh', 'Mohammadreza Nakhaei'
]

# Process datasets
trade_values = {}
for name in names:
    df = load_and_preprocess(f'{name}.csv')  # Load individual datasets without 'GBP-IRR ratio'
    if df is not None and price_data is not None:
        synced_df = sync_gbp_irr_ratio(price_data, df)  # Sync 'GBP-IRR ratio' with the dataset
        trade_values[name] = calculate_trade_value(synced_df)

# Print results
for name, value in trade_values.items():
    print(f"{name}'s account trade value is: {int(value):,}£\n")

# Print total trade value
total_trade_value = sum(trade_values.values())
print(f"Total trade value is: {int(total_trade_value):,}£\n")
