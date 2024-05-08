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
    
    # drop rows with a 0 value in the column last
    # df = df[df['last'] != 0]
    # remove parentheses and the letter c or s from the column last
    df['last'] = df['last'].astype(str).str.replace(r'\([cs]\)', '', regex=True).replace(r' ', '', regex=True)

    # convert the last column to float
    df['last'] = df['last'].astype(float)
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
    # df_compA = pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    files_2019, files_2020, files_2021, files_2022, files_2023 = generate_path(2019), generate_path(2020), generate_path(2021), generate_path(2022), generate_path(2023)
    # files_2019 = glob.glob(f'data/2019/*')
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
            if 'peapme' in path:
                df_peapme.loc[date] = path
        print(f"Year {date.year} done.")
                
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"create_path_df: Execution time: {elapsed_time:.6f} seconds")

    return df_compA, df_compB, df_amsterdam, df_peapme


def feed_companies(df_compA, df_compB, df_amsterdam, df_peapme, year):
    print(f"feed companies for year {year} starting...")
    start_time = time.time()
    df_compA = df_compA[df_compA.index.year == year]
    latest_row_compA = df_compA.loc[df_compA.index.max()]
    df_input_compA = pd.read_pickle(f'data/{str(year)}/{latest_row_compA["path"]}.bz2')
    dfcompA = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
    for row in df_input_compA.iterrows():
        query = db.raw_query(f"SELECT * FROM companies WHERE symbol = '{row[1]['symbol']}'")
        if query:
            db.execute(f"UPDATE companies SET symbol = '{row[1]['symbol']}' WHERE symbol = '{row[1]['symbol']}'")
        else:
            new_row = {'name': row[1]['name'], 'mid': 1, 'symbol': row[1]['symbol'], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': False, 'sector': 0}
            dfcompA = pd.concat([dfcompA, pd.DataFrame(new_row, index=[0])], ignore_index=True)
    db.df_write_optimized(dfcompA, table="companies")
    db.commit()
    
    df_compB = df_compB[df_compB.index.year == year]
    latest_row_compB = df_compB.loc[df_compB.index.max()]
    df_input_compB = pd.read_pickle(f'data/{str(year)}/{latest_row_compB["path"]}.bz2')
    dfcompB = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
    for row in df_input_compB.iterrows():
        query = db.raw_query(f"SELECT * FROM companies WHERE symbol = '{row[1]['symbol']}'")
        if query:
            db.execute(f"UPDATE companies SET symbol = '{row[1]['symbol']}' WHERE symbol = '{row[1]['symbol']}'")
        else:
            new_row = {'name': row[1]['name'], 'mid': 1, 'symbol': row[1]['symbol'], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': False, 'sector': 0}
            dfcompB = pd.concat([dfcompB, pd.DataFrame(new_row, index=[0])], ignore_index=True)
    db.df_write_optimized(dfcompB, table="companies")
    db.commit()
    
    df_amsterdam = df_amsterdam[df_amsterdam.index.year == year]
    latest_row_amsterdam = df_amsterdam.loc[df_amsterdam.index.max()]
    df_input_amsterdam = pd.read_pickle(f'data/{str(year)}/{latest_row_amsterdam["path"]}.bz2')
    dfamsterdam = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
    for row in df_input_amsterdam.iterrows():
        query = db.raw_query(f"SELECT * FROM companies WHERE symbol = '{row[1]['symbol']}'")
        if query:
            db.execute(f"UPDATE companies SET symbol = '{row[1]['symbol']}' WHERE symbol = '{row[1]['symbol']}'")
        else:
            new_row = {'name': row[1]['name'], 'mid': 1, 'symbol': row[1]['symbol'], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': False, 'sector': 0}
            dfamsterdam = pd.concat([dfamsterdam, pd.DataFrame(new_row, index=[0])], ignore_index=True)
    db.df_write_optimized(dfamsterdam, table="companies")
    db.commit()
    
    if year >= 2021:
        df_peapme = df_peapme[df_peapme.index.year == year]
        latest_row_peapme = df_peapme.loc[df_peapme.index.max()]
        df_input_peapme = pd.read_pickle(f'data/{str(year)}/{latest_row_peapme["path"]}.bz2')
        dfpeapme = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
        for row in df_input_peapme.iterrows():
            query = db.raw_query(f"SELECT * FROM companies WHERE symbol = '{row[1]['symbol']}'")
            if query:
                db.execute(f"UPDATE companies SET symbol = '{row[1]['symbol']}' WHERE symbol = '{row[1]['symbol']}'")
            else:
                new_row = {'name': row[1]['name'], 'mid': 1, 'symbol': row[1]['symbol'], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': True, 'sector': 0}
                dfpeapme = pd.concat([dfpeapme, pd.DataFrame(new_row, index=[0])], ignore_index=True)
        db.df_write_optimized(dfpeapme, table="companies")
        db.commit()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed companies: Execution time: {elapsed_time:.6f} seconds")    
        
    
def feed_stocks(path_df, market_id, companies_map, companies_map_with_name):
    # stocks table : date: datetime, cid: int, value: float, volume: bigint
    start_time = time.time()
    print(f"feed stocks for market {market_id} starting...")
    for file in path_df.iterrows():
        # print(file)
        path = file[1]['path'].split('/')[-1]
        path_without_letters  = ''.join([i for i in path if not i.isalpha()])
        date = dateutil.parser.parse(path_without_letters)
        year = str(date.year)
        df_input = pd.read_pickle(f'data/{year}/{file[1]["path"]}.bz2')
        df_input = clean_df(df_input)

        df_output = pd.DataFrame({'date': pd.Series([], dtype='datetime64[ns]'), 'cid': pd.Series([], dtype='int'), 'value': pd.Series([], dtype='float'), 'volume': pd.Series([], dtype='int')})
        for row in df_input.iterrows():
            if row[0] not in companies_map:
                cid = len(companies_map) + 1
                companies_map[row[0]] = cid
                companies_map_with_name[row[0]] = (cid, row[1]['name'])
            else:   
                cid = companies_map[row[0]]
            new_row = {'date': date, 'cid': cid, 'value': float(row[1]['last']), 'volume': int(row[1]['volume'])}
            df_output = pd.concat([df_output, pd.DataFrame(new_row, index=[0])], ignore_index=True)
        db.df_write_optimized(df_output, table="stocks")
        db.commit()               
    
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed_stocks for market {market_id}: Execution time: {elapsed_time:.6f} seconds")
    return companies_map, companies_map_with_name

    
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
    
def generate_companies_map(query_result):
    companies_map = {}
    companies_map_with_name = {}
    for result in query_result:
        result = result[0]
        # result is of type string like "(1, 'AAPL', 'Amazon')"
        result = result[1:-1].split(',')
        companies_map[result[1]] = int(result[0])
        companies_map_with_name[result[1]] = (int(result[0]), result[2])
    return companies_map, companies_map_with_name

def add_new_companies(companies_map, old_len, market_id):
    # companies table : name: str, mid: int, symbol: str, symbol_nf: str, isin: str, reuters: str, boursorama: str, pea: bool, sector: int
    df = pd.DataFrame({'name': pd.Series([], dtype='str'), 'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'), 'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')})
    for i in range(old_len, len(companies_map)):
        pea = True if market_id == 1 else False
        new_row = {'name': companies_map[i][1], 'mid': market_id, 'symbol': companies_map[i][0], 'symbol_nf': None, 'isin': None, 'reuters': None, 'boursorama': None, 'pea': pea, 'sector': 0}
        df = pd.concat([df, pd.DataFrame(new_row, index=[0])], ignore_index=True)
    db.df_write_optimized(df, table="companies")
    db.commit()

if __name__ == '__main__':
    print("Start")
    start_time = time.time()
    db.clean_all_tables()

    df_compA, df_compB, df_amsterdam, df_peapme = create_path_df()
    for year in range(2019, 2024):
        feed_companies(df_compA, df_compB, df_amsterdam, df_peapme, year)
    feed_companies(df_compA, df_compB, df_amsterdam, df_peapme, 2019)
    companies = db.raw_query('''SELECT (id, symbol, name) from companies;''')
    companies_map, companies_map_with_name = generate_companies_map(companies)
    
    companies_map, companies_map_with_name = feed_stocks(df_compA, 7, companies_map, companies_map_with_name)
    add_new_companies(companies_map, len(companies_map), 7)
    companies_map, companies_map_with_name = feed_stocks(df_compB, 8, companies_map, companies_map_with_name)
    add_new_companies(companies_map, len(companies_map), 8)
    companies_map, companies_map_with_name = feed_stocks(df_amsterdam, 6, companies_map, companies_map_with_name)
    add_new_companies(companies_map, len(companies_map), 6)
    companies_map, companies_map_with_name = feed_stocks(df_peapme, 1, companies_map, companies_map_with_name)
    add_new_companies(companies_map, len(companies_map), 1)
    
    feed_daystocks()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.6f} seconds")

            
    # print("---- LOGS ----")
    # with open("/tmp/bourse.log", "r") as file:
    #     print(file.read())
    print("Done")
