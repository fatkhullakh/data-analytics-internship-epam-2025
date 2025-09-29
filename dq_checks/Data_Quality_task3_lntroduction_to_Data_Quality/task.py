import pandas as pd
df = pd.read_parquet('car_prices.parquet')
df.to_csv('car_prices.csv', index=False)
johnjohn33345