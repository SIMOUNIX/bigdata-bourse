import glob
import pandas as pd
from datetime import datetime
import re

# clean a dataframe
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
            
for year in range(2021, 2024):
    # Call iterate_files with category 'All' to ensure all dataframes are returned
    category = 'peapme'
    returned_dfs = iterate_files(year, category=category)
    
    # unpack the right dfs based on the category
    if category == 'All':
        df_compA, df_compB, df_amsterdam, df_peapme = returned_dfs
    elif category == 'compA':
        df_compA = returned_dfs[0]
        df_compB, df_amsterdam, df_peapme = None, None, None
    elif category == 'compB':
        df_compB = returned_dfs[1]
        df_compA, df_amsterdam, df_peapme = None, None, None
    elif category == 'amsterdam':
        df_amsterdam = returned_dfs[2]
        df_compA, df_compB, df_peapme = None, None, None
    elif category == 'peapme':
        df_peapme = returned_dfs[3]
        df_compA, df_compB, df_amsterdam = None, None, None
    
    print("compA for year {}".format(year))
    if df_compA is not None:
        print(df_compA.head())
    else:
        print("No data found for compA")

    print("compB for year {}".format(year))
    if df_compB is not None:
        print(df_compB.head())
    else:
        print("No data found for compB")
    
    print("amsterdam for year {}".format(year))
    if df_amsterdam is not None:
        print(df_amsterdam.head())
    else:
        print("No data found for amsterdam")
    
    if year >= 2021:
        print("peapme for year {}".format(year))
        if df_peapme is not None:
            print(df_peapme.head())
        else:
            print("No data found for peapme")
    
    print('Year {} done dude on god skibidi'.format(year))



# old function in case the new one doesn't work
# def iterate_files(year, category='All'):
#     if year == 2019:
#         if category == 'All':
#             files_compA = glob.glob(r"boursorama/2019/compA*")
#             files_compB = glob.glob(r"boursorama/2019/compB*")
#             files_amsterdam = glob.glob(r"boursorama/2019/amsterdam*")
#         elif category == 'compA':
#             files_compA = glob.glob(r"boursorama/2019/compA*")
#             files_compB = None
#             files_amsterdam = None
#         elif category == 'compB':
#             files_compA = None
#             files_compB = glob.glob(r"boursorama/2019/compB*")
#             files_amsterdam = None
#         elif category == 'amsterdam':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = glob.glob(r"boursorama/2019/amsterdam*")
#     elif year == 2020:
#         if category == 'All':
#             files_compA = glob.glob(r"boursorama/2020/compA*")
#             files_compB = glob.glob(r"boursorama/2020/compB*")
#             files_amsterdam = glob.glob(r"boursorama/2020/amsterdam*")
#         elif category == 'compA':
#             files_compA = glob.glob(r"boursorama/2020/compA*")
#             files_compB = None
#             files_amsterdam = None
#         elif category == 'compB':
#             files_compA = None
#             files_compB = glob.glob(r"boursorama/2020/compB*")
#             files_amsterdam = None
#         elif category == 'amsterdam':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = glob.glob(r"boursorama/2020/amsterdam*")
#     elif year == 2021:
#         if category == 'All':
#             files_compA = glob.glob(r"boursorama/2021/compA*")
#             files_compB = glob.glob(r"boursorama/2021/compB*")
#             files_amsterdam = glob.glob(r"boursorama/2021/amsterdam*")
#             files_peapme = glob.glob(r"boursorama/2021/peapme*")
#         elif category == 'compA':
#             files_compA = glob.glob(r"boursorama/2021/compA*")
#             files_compB = None
#             files_amsterdam = None
#             files_peapme = None
#         elif category == 'compB':
#             files_compA = None
#             files_compB = glob.glob(r"boursorama/2021/compB*")
#             files_amsterdam = None
#             files_peapme = None
#         elif category == 'amsterdam':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = glob.glob(r"boursorama/2021/amsterdam*")
#             files_peapme = None
#         elif category == 'peapme':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = None
#             files_peapme = glob.glob(r"boursorama/2021/peapme*")
#     elif year == 2022:
#         if category == 'All':
#             files_compA = glob.glob(r"boursorama/2022/compA*")
#             files_compB = glob.glob(r"boursorama/2022/compB*")
#             files_amsterdam = glob.glob(r"boursorama/2022/amsterdam*")
#         elif category == 'compA':
#             files_compA = glob.glob(r"boursorama/2022/compA*")
#             files_compB = None
#             files_amsterdam = None
#         elif category == 'compB':
#             files_compA = None
#             files_compB = glob.glob(r"boursorama/2022/compB*")
#             files_amsterdam = None
#         elif category == 'amsterdam':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = glob.glob(r"boursorama/2022/amsterdam*")
#     elif year == 2023:
#         if category == 'All':
#             files_compA = glob.glob(r"boursorama/2023/compA*")
#             files_compB = glob.glob(r"boursorama/2023/compB*")
#             files_amsterdam = glob.glob(r"boursorama/2023/amsterdam*")
#         elif category == 'compA':
#             files_compA = glob.glob(r"boursorama/2023/compA*")
#             files_compB = None
#             files_amsterdam = None
#         elif category == 'compB':
#             files_compA = None
#             files_compB = glob.glob(r"boursorama/2023/compB*")
#             files_amsterdam = None
#         elif category == 'amsterdam':
#             files_compA = None
#             files_compB = None
#             files_amsterdam = glob.glob(r"boursorama/2023/amsterdam*")
#     if category == 'All':
#         df_compA = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compA})
#         df_compA_clean = clean_df(df_compA)
#         df_compB = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compB})
#         df_compB_clean = clean_df(df_compB)
#         df_amsterdam = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[26:]):pd.read_pickle(file) for file in files_amsterdam})
#         df_amsterdam_clean = clean_df(df_amsterdam)
#         if year >= 2021:
#             df_peapme = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[23:]):pd.read_pickle(file) for file in files_peapme})
#             df_peapme_clean = clean_df(df_peapme)
#         else:
#             df_peapme = None
#         # save the cleaned dataframes in the folder boursorama_clean
#         df_compA_clean.to_pickle('boursorama_clean/compA_clean.pkl')
#         df_compB_clean.to_pickle('boursorama_clean/compB_clean.pkl')
#         df_amsterdam_clean.to_pickle('boursorama_clean/amsterdam_clean.pkl')
#         if year >= 2021:
#             df_peapme_clean.to_pickle('boursorama_clean/peapme_clean.pkl')
#         else:
#             df_peapme_clean = None
#         print('All files saved dude on god skibidi')
#     elif category == 'compA':
#         df_compA = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compA})
#         df_compA_clean = clean_df(df_compA)
#         # save the cleaned dataframes in the folder boursorama_clean
#         df_compA_clean.to_pickle('boursorama_clean/compA_clean.pkl')
#         print('Files of compA saved dude on god skibidi')
#     elif category == 'compB':
#         df_compB = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compB})
#         df_compB_clean = clean_df(df_compB)
#         # save the cleaned dataframes in the folder boursorama_clean
#         df_compB_clean.to_pickle('boursorama_clean/compB_clean.pkl')
#         print('Files of compB saved dude on god skibidi')
#     elif category == 'amsterdam':
#         df_amsterdam = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[26:]):pd.read_pickle(file) for file in files_amsterdam})
#         df_amsterdam_clean = clean_df(df_amsterdam)
#         # save the cleaned dataframes in the folder boursorama_clean
#         df_amsterdam_clean.to_pickle('boursorama_clean/amsterdam_clean.pkl')
#         print('Files of amsterdam saved dude on god skibidi')
#     elif category == 'peapme':
#         df_peapme = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[23:]):pd.read_pickle(file) for file in files_peapme})
#         df_peapme_clean = clean_df(df_peapme)
#         # save the cleaned dataframes in the folder boursorama_clean
#         df_peapme_clean.to_pickle('boursorama_clean/peapme_clean.pkl')
#         print('Files of peapme saved dude on god skibidi')
#     return df_compA_clean, df_compB_clean, df_amsterdam_clean, df_peapme_clean
