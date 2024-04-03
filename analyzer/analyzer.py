import pandas as pd
import numpy as np
import sklearn

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

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
        


if __name__ == '__main__':
    store_file("compA 2020-01-01 09:02:02.532411", "boursorama")
    print("Done")
