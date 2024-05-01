import dateutil.parser
import pandas as pd
import numpy as np
import glob
import os
import dateutil

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def clean_df(df):
    df = df.dropna()
    df = df.drop(columns=['symbol'])
    
    # drop rows with a 0 value in the column last
    df = df[df['last'] != 0]
    print('Clean done dude on god skibidi')
    
    # get the oldest name of the company for each symbol
    oldest_names = df.groupby(level=1)['name'].transform('max')
    # Updating the 'name' column with the oldest 'name'
    df['name'] = oldest_names
    return df

def create_path_df():
    i = 0
    # create 4 empty df with timestamp as index and 1 column being a path (string)
    #df_compA, df_compB, df_amsterdam, df_peapme = pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    df_compA = pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    #files_2019, files_2020, files_2021, files_2022, files_2023 = glob.glob(f'data/2019/*'), glob.glob(f'data/2020/*'), glob.glob(f'data/2021/*'), glob.glob(f'data/2022/*'), glob.glob(f'data/2023/*')
    files_2019 = glob.glob(f'data/2019/*')
    for file in (files_2019):#, files_2020, files_2021, files_2022, files_2023):
        if i == 100:
            break
        #for file in files:
        path = file.split('/')[-1]
        path = path[:-4]
        path_without_letters  = ''.join([i for i in path if not i.isalpha()])
        date = dateutil.parser.parse(path_without_letters)
        if 'compA' in path:
            df_compA.loc[date] = path
        # elif 'compB' in path:
        #     df_compB.loc[date] = path
        # elif 'amsterdam' in path:
        #     df_amsterdam.loc[date] = path
        # elif 'peapme' in path:
        #     df_peapme.loc[date] = path
        i += 1
    return df_compA#, df_compB, df_amsterdam, df_peapme

def feed_companies(df_compA):#, df_compB, df_amsterdam, df_peapme):
    # companies db : id:smallint, name:varchar, market_id:smallint, symbol:varchar, symbol_nf:varchar, isin:char(12), reuters:varchar, boursorama:varchar, pea:boolean, sector:integer
    market_map = {1 : "euronx", 2 : "lse",
                        3: "milano", 4: "dbx",
                        5: "mercados",6:"amsterdam",
                        7:"compA",8:"compB",
                        9:"xetra",10:"bruxelle"}
    
    # getting the last day to have the last company names
    # checked the data and all the rows are unique (no duplicates)
    last_rows = df_compA.loc['2019-12-31']
    df = pd.read_pickle(f'data/2019/{last_rows["path"][0]}.bz2')
    for row in df.iterrows():
        df = pd.DataFrame(columns=['id', 'name', 'market_id', 'symbol', 'symbol_nf', 'isin', 'reuters', 'boursorama', 'pea', 'sector'])
        df['name'] = row[1]['name']
        df['mid'] = 7
        df['symbol'] = row[1]['symbol']
        df['symbol_nf'] = None
        df['isin'] = None
        df['reuters'] = None
        df['boursorama'] = None
        df['pea'] = None
        df['sector'] = None
        # insertion in db does not work, idk why
        db.df_write(df, table="companies", if_exists="append", index=False)
        db.commit()

# not tested
def feed_stocks(df):
    # stocks db : date: timestamptz, cid: smallint, value: float4, volume:int
    for row in df.iterrows():
        df = pd.DataFrame(columns=['date', 'cid', 'value', 'volume'])
        df['date'] = row[0]
        df['cid'] = 1
        df['value'] = row[1]['last']
        df['volume'] = row[1]['volume']
        db.df_write(df, table="stocks", if_exists="append", index=False)
        db.commit()
        
def feed_daystocks(df):
    pass

def file_done(path):
    pass

if __name__ == '__main__':
    df_compA = create_path_df()
    feed_companies(df_compA)
    print("Done")


# def store_file(name, website):
#     name += ".bz2"
#     query = tsdb.is_file_done(name)
#     print(query)
#     if query[0][0] is not None:
#         return
#     if website.lower() == "boursorama":
#         try:
#             df = pd.read_pickle("data/" + name)  # is this dir ok for you ?
#             print(df.head())
#         except:
#             year = name.split()[1].split("-")[0]
#             df = pd.read_pickle("data/" + year + "/" + name)
#             print(df.info())
#             print(df.head())
#         #db.df_write(df, "file_done")
