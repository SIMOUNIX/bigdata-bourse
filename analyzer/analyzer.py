import pandas as pd
import numpy as np
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

# iterate through all files and call store_file
def iterate_files():
    files_2019 = glob.glob(r"boursorama/2019/*")
    files_2020 = glob.glob(r"boursorama/2020/*")
    files_2021 = glob.glob(r"boursorama/2021/*")
    files_2022 = glob.glob(r"boursorama/2022/*")
    files_2023 = glob.glob(r"boursorama/2023/*")
    for file in files_2019:
        name = file.split("/")[1]
        website = file.split("/")[2]
        print(name, website)
        # store_file(name, website)
    for file in files_2020:
        name = file.split("/")[1]
        website = file.split("/")[2]
        print(name, website)
        # store_file(name, website)
    for file in files_2021:
        name = file.split("/")[1]
        website = file.split("/")[2]
        print(name, website)
        # store_file(name, website)
    for file in files_2022:
        name = file.split("/")[1]
        website = file.split("/")[2]
        print(name, website)
        # store_file(name, website)
    for file in files_2023:
        name = file.split("/")[1]
        website = file.split("/")[2]
        print(name, website)
        # store_file(name, website)
    return

def store_file(name, website):
    name += ".bz2"
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("data/" + name)  # is this dir ok for you ?
            print(df.head())
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("data/" + year + "/" + name)
            print(df.info())
            print(df.head())
        #db.df_write(df, "file_done")

def clean_data(df):
    df = df.dropna()
    df = df.drop_duplicates()
    return df


if __name__ == '__main__':
    #store_file("compA 2020-01-01 09:02:02.532411", "boursorama")
    print("Done")
