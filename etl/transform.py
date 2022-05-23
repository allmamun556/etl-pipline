import db
import numpy as np

#from etl.db import db_load
#import pandas as pd



''' Transform price which has BTC, ETH, XRP and LTC in the dataset and 
convert price great bretain pound(GBP) '''


def convert_us_to_gbp(crypto_df, assetsCode):
    # coverting open, close, high and low price of crypto currencies into GBP values since current price is in Dollars
    # if currency belong to this list ['BTC','ETH','XRP','LTC']
    crypto_df['open'] = crypto_df[['open', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
    crypto_df['close'] = crypto_df[['close', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
    crypto_df['high'] = crypto_df[['high', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)
    crypto_df['low'] = crypto_df[['low', 'asset']].apply(lambda x: (float(x[0]) * 0.80) if x[1] in assetsCode else np.nan, axis=1)

    # dropping rows with null values by asset column

    crypto_df.dropna(inplace=True)

    # reset the data frame index
    crypto_df.reset_index(drop=True ,inplace=True)
    crypto_df.head()
    #print(crypto_df)
    return crypto_df

#convert_us_to_gbp(db.db_load(),['BTC','ETH','XRP','LTC'])


def required_column(crypto_df):
    crypto_df.drop(labels=['slug', 'ranknow', 'volume', 'market', 'close_ratio', 'spread'], inplace=True, axis=1)
    #print(crypto_df.head())
    return crypto_df



#final_data=(required_column(convert_us_to_gbp(db.db_load(),['BTC','ETH','XRP','LTC'])))
#print(type(final_data))   
