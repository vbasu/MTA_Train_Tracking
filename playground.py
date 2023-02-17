import pandas as pd

df = pd.read_csv('Data/Turnstile_Usage_Data__2020.csv')
df = df[df['Unit'] == 'R293']
print(df)