import pandas as pd

def db_load():
 crypto_df = pd.read_csv('data/crypto-markets.csv')
 crypto_df.head()
 #print(crypto_df)
 return crypto_df


#db_load()
