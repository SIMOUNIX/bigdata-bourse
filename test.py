import glob
import os
import pandas as pd
import dateutil

def iterate_files(year):
    if year == 2019:
        files_compA = glob.glob(r"boursorama/2019/compA*")
        files_compB = glob.glob(r"boursorama/2019/compB*")
        files_amsterdam = glob.glob(r"boursorama/2019/amsterdam*")
    elif year == 2020:
        files_compA = glob.glob(r"boursorama/2020/compA*")
        files_compB = glob.glob(r"boursorama/2020/compB*")
        files_amsterdam = glob.glob(r"boursorama/2020/amsterdam*")
    elif year == 2021:
        files_compA = glob.glob(r"boursorama/2021/compA*")
        files_compB = glob.glob(r"boursorama/2021/compB*")
        files_amsterdam = glob.glob(r"boursorama/2021/amsterdam*")
        files_peapme = glob.glob(r"boursorama/2021/peapme*")
    elif year == 2022:
        files_compA = glob.glob(r"boursorama/2022/compA*")
        files_compB = glob.glob(r"boursorama/2022/compB*")
        files_amsterdam = glob.glob(r"boursorama/2022/amsterdam*")
        files_peapme = glob.glob(r"boursorama/2022/peapme*")
    elif year == 2023:
        files_compA = glob.glob(r"boursorama/2023/compA*")
        files_compB = glob.glob(r"boursorama/2023/compB*")
        files_amsterdam = glob.glob(r"boursorama/2023/amsterdam*")
        files_peapme = glob.glob(r"boursorama/2023/peapme*")
    # concatenate all files into a single df
    # print the files[0] wihtout the last 4 characters
    # print(files[0][:-4].replace('_', ':')[22:])
    df_compA = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compA})
    df_compB = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[22:]):pd.read_pickle(file) for file in files_compB})
    df_amsterdam = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[26:]):pd.read_pickle(file) for file in files_amsterdam})
    if year >= 2021:
        df_peapme = pd.concat({dateutil.parser.parse((file[:-4].replace('_', ':'))[23:]):pd.read_pickle(file) for file in files_peapme})
    else:
        df_peapme = None
    return df_compA, df_compB, df_amsterdam, df_peapme
        
            
df_compA_2019, df_compB_2019, df_amsterdam_2019, df_peapme_2019 = iterate_files(2019)
print(df_compA_2019.head())
