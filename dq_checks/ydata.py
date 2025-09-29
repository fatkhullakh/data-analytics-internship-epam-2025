import pandas as pd
from ydata_profiling import ProfileReport

df = pd.read_csv('car_prices.csv')
profile = ProfileReport(df, title="Car Prices Profiling", explorative=True)
profile.to_file('car_prices_profile.html')