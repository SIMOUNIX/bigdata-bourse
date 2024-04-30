import dateutil.parser
import pandas as pd
import numpy as np
import glob
import os
import dateutil

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

# iterate through all files and call store_file
def clean_df(df):
    # print(df.head())
    # print(df.info())
    # print(df.describe())
    # print(df.shape)
    df = df.dropna()
    #df = df.drop_duplicates() # this drops 4 million rows, so sus
    df = df.drop(columns=['symbol'])
    
    # drop rows with a 0 value in the column last
    df = df[df['last'] != 0]
    print('Clean done dude on god skibidi')
    
    # get the oldest name of the company for each symbol
    oldest_names = df.groupby(level=1)['name'].transform('max')
    # Updating the 'name' column with the oldest 'name'
    df['name'] = oldest_names
    return df

def iterate_files(year, category='All'):
    folders = {
        2019: 'boursorama/2019/',
        2020: 'boursorama/2020/',
        2021: 'boursorama/2021/',
        2022: 'boursorama/2022/',
        2023: 'boursorama/2023/'
    }
    
    if category == 'All':
        file_paths = {
            'compA': f'{folders[year]}compA*',
            'compB': f'{folders[year]}compB*',
            'amsterdam': f'{folders[year]}amsterdam*',
            'peapme': f'{folders[year]}peapme*' if year >= 2021 else None
        }
    else:
        file_paths = {category: f'{folders[year]}{category}*'}
        for cat in ('compA', 'compB', 'amsterdam', 'peapme'):
            if cat != category:
                file_paths[cat] = None
    
    df_clean = {}
    for cat, path in file_paths.items():
        if path:
            files = glob.glob(path)
            if files or (category == 'peapme' and year < 2021):  # Check if files exist or if it's peapme before 2021
                data = {}
                for file in files:
                    match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+', file)
                    if match:
                        date_str = match.group()
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                        data[date_obj] = pd.read_pickle(file)
                
                df = pd.concat(data)
                df_clean[cat] = clean_df(df)
                print(df_clean[cat].head())
                #df_clean[cat].to_pickle(f'boursorama_clean/{cat}_{year}_clean.pkl')
                print(f'Files of {cat} of year {year} saved dude on god skibidi')
            else:
                print(f'No files found for {cat}')
                df_clean[cat] = None
        else:
            df_clean[cat] = None
            
    print('All files saved dude on god skibidi')
    
    return tuple(df_clean.values())

market_map = map = {1 : "euronx", 2 : "lse", 3: "milano", 4: "dbx", 5: "mercados",6:"amsterdam",7:"compA",8:"compB",9:"xetra",10:"bruxelle"}

def create_path_df():
    # create 4 empty df with timestamp as index and 1 column being a path (string)
    #df_compA, df_compB, df_amsterdam, df_peapme = pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([])), pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    df_compA = pd.DataFrame(columns=['path'], index=pd.to_datetime([]))
    #files_2019, files_2020, files_2021, files_2022, files_2023 = glob.glob(f'data/2019/*'), glob.glob(f'data/2020/*'), glob.glob(f'data/2021/*'), glob.glob(f'data/2022/*'), glob.glob(f'data/2023/*')
    files_2019 = glob.glob(f'data/2019/*')
    for files in (files_2019):#, files_2020, files_2021, files_2022, files_2023):
        for file in files:
            print(path)
            path = file.split('/')[-1]
            path = path[:-4]
            path_without_letters  = ''.join([i for i in path if not i.isalpha()])
            print(path_without_letters[1:-4].replace('_', ':'))
            date = dateutil.parser.parse(path_without_letters[1:-4].replace('_', ':'))
            if 'compA' in path:
                df_compA.loc[date] = path
            # elif 'compB' in path:
            #     df_compB.loc[date] = path
            # elif 'amsterdam' in path:
            #     df_amsterdam.loc[date] = path
            # elif 'peapme' in path:
            #     df_peapme.loc[date] = path
        return
    return df_compA#, df_compB, df_amsterdam, df_peapme
    
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

def feed_companies(df_compA):#, df_compB, df_amsterdam, df_peapme):
    # get the max index of compA:
    max_index = df_compA.index.max()
    print(max_index)
        


def clean_data(df):
    df = df.dropna()
    df = df.drop_duplicates()
    return df


if __name__ == '__main__':
    df_compA = create_path_df()
    feed_companies(df_compA)
    print("Done")
