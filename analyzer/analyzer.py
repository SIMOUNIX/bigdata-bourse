import dateutil.parser
import pandas as pd
import numpy as np
import glob
import os
import dateutil
import time
from concurrent.futures import ThreadPoolExecutor

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

# GENERATING THE PATHS DATAFRAMES

def generate_path(year):
    for path, _, files in os.walk(f'data/{year}'):
        for file in files:
            yield os.path.join(path, file)

def process_file_path(file):
    path = file.split('/')[-1] # get the file name
    path = path[:-4] # remove the extension .bz2
    path_without_letters = ''.join([i for i in path if not i.isalpha()]) # remove the letters to keep only the date
    date = dateutil.parser.parse(path_without_letters) # convert the string to a datetime
    return file, date

def create_path_df():
    start_time = time.time()
    print("create_path_df starting...")

    # Create a dictionary to store paths for each category
    categories = {'compA': [], 'compB': [], 'amsterdam': [], 'peapme': []}

    # Function to process files and update the dictionary
    def update_categories(file, date):
        for category in categories:
            if category in file:
                categories[category].append((file, date))
                break

    # Generate file paths
    file_paths = [file for year in range(2019, 2024) for file in generate_path(year)]

    # Process files in parallel
    with ThreadPoolExecutor() as executor:
        for file, date in executor.map(process_file_path, file_paths):
            update_categories(file, date)

    # Convert dictionary to DataFrames
    dfs = {
        category: pd.DataFrame({'path': [path for path, _ in paths]}, 
                            index=pd.to_datetime([date for _, date in paths]))
        for category, paths in categories.items()
    }

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"create_path_df: Execution time: {elapsed_time:.6f} seconds")

    return dfs['compA'], dfs['compB'], dfs['amsterdam'], dfs['peapme']

# FEEDING THE DATABASES

def clean_df(df):
    df = df.dropna()
    # used in feed_stocks so symbol and name are not needed
    df = df.drop(columns=['symbol', 'name'])
    # removing the (c) and (s) from the last column and converting it to float (ex : 123.54(c) -> 123.54)
    df['last'] = df['last'].astype(str).str.replace(r'\([cs]\)', '', regex=True).replace(r' ', '', regex=True)
    df['last'] = df['last'].astype(float)
    # converting the volume to int
    df['volume'] = df['volume'].astype(int)
    return df

def feed_companies(path_df, mid):
    start_time = time.time()
    print(f"feed companies for market {mid} starting...")
    pea = (mid == 1)
    
    # keep the last index of each day
    path_df = path_df.resample('D').last()
    # droping the days with no data -> stock market closed
    path_df = path_df.dropna()
    
    # creating the companies dataframe following the database schema
    df_output = pd.DataFrame({'name': pd.Series([], dtype='str'),
        'mid': pd.Series([], dtype='int'), 'symbol': pd.Series([], dtype='str'),
        'symbol_nf': pd.Series([], dtype='str'), 'isin': pd.Series([], dtype='str'), 
        'reuters': pd.Series([], dtype='str'), 'boursorama': pd.Series([], dtype='str'), 
        'pea': pd.Series([], dtype='bool'), 'sector': pd.Series([], dtype='int')
    })
    
    for _, file in path_df.iterrows():
        df = pd.read_pickle(file[0]) # index: symbol - columns: name, symbol, last, volume
        df.drop(columns=['last', 'volume'], inplace=True) # removing the columns not needed
        
        for symbol, row in df.iterrows(): # symbol is the index of the dataframe
            name = row['name']
            
            # checking if the company already exists in the dataframe
            exisiting_company = df_output[df_output['symbol'] == symbol]
            
            if not exisiting_company.empty: # if the company already exists, we update the name if it changed
                existing_company_name = exisiting_company.iloc[0]['name']
                if existing_company_name != name:
                    df_output.loc[df_output['symbol'] == symbol, 'name'] = name
            else: # if the company does not exist, we add it to the dataframe
                new_row = {'name': name, 'mid': mid, 'symbol': symbol, 'symbol_nf': '', 'isin': '', 'reuters': '', 'boursorama': '', 'pea': pea, 'sector': 0}
                df_output = pd.concat([df_output, pd.DataFrame(new_row, index=[0])], ignore_index=True)
        
    db.df_write_optimized(df_output, table="companies")
    db.commit()
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed companies: Execution time: {elapsed_time:.6f} seconds")

    
def feed_stocks(path_df, mid):
    start_time = time.time()
    print(f"feed stocks for market {mid} starting...")
    
    for date, file in path_df.iterrows(): # date is the index of the dataframe
        # getting the companies ids and symbols
        cids = db.df_query(f"SELECT id, symbol FROM companies WHERE mid = {mid}") 
        cids = [cid for cid in cids]
        cids = cids[0] # index: id - columns: symbol
        
        # cleaning and formatting the stocks dataframe
        df = clean_df(pd.read_pickle(file[0])) # index: symbol - columns: last, volume
        
        # merging the company ids with their stock values by symbol (if the symbol is the same than we merge the rows)
        merged_df = pd.merge(cids, df, left_on='symbol', right_index=True, how='inner')
        merged_df = merged_df.rename(columns={'id': 'cid', 'last': 'value'}) # renaming the columns
        merged_df['date'] = date # adding the date column
        merged_df = merged_df[['date', 'cid', 'value', 'volume']] # reordering columns
        
        db.df_write_optimized(merged_df, table="stocks")
        db.commit()
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"feed_stocks: Execution time: {elapsed_time:.6f} seconds")
        
def feed_daystocks():
    start_time = time.time()
    print("feed daystocks starting...")
    # filling the daystocks table with the data from the stocks table
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
    
    # UNCOMMENT THE FOLLOWING LINES TO FEED THE DATABASES
    # db.clean_all_tables()

    # df_compA, df_compB, df_amsterdam, df_peapme = create_path_df()
    
    # feed_companies(df_compA, 7)
    # feed_companies(df_compB, 8)
    # feed_companies(df_amsterdam, 9)
    # feed_companies(df_peapme, 1)
    
    # feed_stocks(df_compA, 7)
    # feed_stocks(df_compB, 8)
    # feed_stocks(df_amsterdam, 9)
    # feed_stocks(df_peapme, 1)
    
    # feed_daystocks()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.6f} seconds")

            
    # print("---- LOGS ----")
    # with open("/tmp/bourse.log", "r") as file:
    #     print(file.read())
    print("Done")
