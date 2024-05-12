import dateutil.parser
import pandas as pd
import numpy as np
import glob
import os
import dateutil
import time

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def clean_df(df):
    df = df.dropna()
    df = df.drop(columns=['symbol'])
    df['last'] = df['last'].astype(str).str.replace(r'\([cs]\)', '', regex=True).replace(r' ', '', regex=True)
    df['last'] = df['last'].astype(float)
    df['volume'] = df['volume'].astype(int)
    return df

def generate_path(year):
    for path, _, files in os.walk(f'data/{year}'):
        for file in files:
            yield os.path.join(path, file)

def create_path_df():
    start_time = time.time()
    print("create_path_df starting...")
    # create 4 empty df with timestamp as index and 1 column being a path (string)
    df_compA, df_compB, df_amsterdam, df_peapme = pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    files_2019, files_2020, files_2021, files_2022, files_2023 = generate_path(2019), generate_path(2020), generate_path(2021), generate_path(2022), generate_path(2023)
    for files in (files_2019, files_2020, files_2021, files_2022, files_2023):
        for file in files:
            path = file.split('/')[-1]
            path = path[:-4]
            path_without_letters  = ''.join([i for i in path if not i.isalpha()])
            date = dateutil.parser.parse(path_without_letters)
            if 'compA' in path:
                df_compA.loc[date] = path
            elif 'compB' in path:
                df_compB.loc[date] = path
            elif 'amsterdam' in path:
                df_amsterdam.loc[date] = path
            elif 'peapme' in path:
                df_peapme.loc[date] = path
                
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"create_path_df: Execution time: {elapsed_time:.6f} seconds")

    return df_compA, df_compB, df_amsterdam, df_peapme

    
def feed_companies(df, mid, year):
    if year < 2021 and mid == 1:
        return
    print(f"feed companies for market {mid} of year {year} starting...")
    start_time = time.time()
    df = df[df.index.year == year]
    latest_row = df.loc[df.index.max()]
    df_input = pd.read_pickle(f'data/{str(year)}/{latest_row["path"]}.bz2')
    df_input.drop(columns=['last', 'volume'], inplace=True)
    df_input.reset_index(drop=True, inplace=True) # dropping the index symbol
    df_output = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
    pea = True if mid == 1 else False
    for row in df_input.iterrows():
        query = db.raw_query(f"SELECT * FROM companies WHERE symbol = '{row[1]['symbol']}'")
        if query:
            db.execute(f"UPDATE companies SET symbol = '{row[1]['symbol']}' WHERE symbol = '{row[1]['symbol']}'")
        else:
            new_row = {'name': row[1]['name'], 'mid': mid, 'symbol': row[1]['symbol'], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': pea, 'sector': 0}
            df_output = pd.concat([df_output, pd.DataFrame(new_row, index=[0])], ignore_index=True)
    db.df_write_optimized(df_output, table="companies")
    db.commit()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed companies: Execution time: {elapsed_time:.6f} seconds")

    
def feed_stocks(path_df, mid):
    start_time = time.time()
    print(f"feed stocks for market {mid} starting...")
    for date, file in path_df.iterrows():
        cids = db.df_query(f"SELECT id, symbol FROM companies WHERE mid = {mid}") # index: id - columns: symbol
        cids = [cid for cid in cids]
        cids = cids[0]
        df = clean_df(pd.read_pickle(f'data/{date.year}/{file[0]}.bz2')) # index: symbol - columns: last, volume
        merged_df = pd.merge(cids, df, left_on='symbol', right_index=True, how='inner')
        merged_df = merged_df.rename(columns={'id': 'cid', 'last': 'value'})
        merged_df['date'] = date
        merged_df = merged_df[['date', 'cid', 'value', 'volume']] # reordering columns
        db.df_write_optimized(merged_df, table="stocks")
        db.commit()
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed_stocks: Execution time: {elapsed_time:.6f} seconds")
        
def feed_daystocks():
    start_time = time.time()
    print("feed daystocks starting...")
    db.execute('''
        INSERT INTO DAYSTOCKS (date, cid, open, close, high, low, volume)
        SELECT
            date::date AS date,
            cid,
            (SELECT value FROM STOCKS s1 WHERE s1.date = MIN(s.date) AND s1.cid = s.cid) AS open,
            (SELECT value FROM STOCKS s2 WHERE s2.date = MAX(s.date) AND s2.cid = s.cid) AS close,
            MAX(value) AS high,
            MIN(value) AS low,
            MAX(volume) AS volume
        FROM STOCKS s
        WHERE cid != 0
        GROUP BY date::date, cid;        ;
        ''')
    db.commit()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed_daystocks: Execution time: {elapsed_time:.6f} seconds")


if __name__ == '__main__':
    print("Start")
    start_time = time.time()
    db.clean_all_tables()

    df_compA, df_compB, df_amsterdam, df_peapme = create_path_df()
    iterations = [(7, df_compA), (8, df_compB), (9, df_amsterdam), (1, df_peapme)]
    years = [2019, 2020, 2021, 2022, 2023]
    
    for mid, df in iterations:
        for year in years:
            feed_companies(df, mid, year)
        feed_stocks(df, mid)
    feed_daystocks()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.6f} seconds")

            
    # print("---- LOGS ----")
    # with open("/tmp/bourse.log", "r") as file:
    #     print(file.read())
    print("Done")
