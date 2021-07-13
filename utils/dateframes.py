import os, sys
import pandas as pd


def isDataframeExist(fileName):
    return os.path.exists(f"{fileName}.csv")


def loadDataFrame(fileName):
    if os.path.exists(f"{fileName}.csv"):
        with open(f"{fileName}.csv", "r") as f:
            try:
                df = pd.read_csv(f, index_col=0)
                df = df.reset_index(drop=True)
                df.columns = df.columns.map(lambda x: x.lower())
                return df
            except:
                pass


def rmDataFrame(filename):
    print(f"remove {filename}.csv and {filename}.txt")

    try:
        os.remove(f"{filename}.csv")
        os.remove(f"{filename}.txt")
    except:
        pass


def saveDataFrame(df, fileName):
    olddf = loadDataFrame(fileName)
    try:
        if not olddf is None and len(olddf) <= len(df) and olddf.compare(df[:len(olddf)]).empty:
            if len(olddf) == len(df):
                # ignore (no changes)
                return

            print(f"append df to {fileName}.csv")
            appenddf = df[len(olddf):]
            df.columns = df.columns.map(lambda x: x.lower())
            with open(f"{fileName}.csv", "a") as f:
                f.write(appenddf.to_csv(header=False))
            with open(f"{fileName}.txt", "a") as f:
                f.write('\n')
                f.write(appenddf.to_string(header=False))
            return
    except:
        pass

    print(f"safe df to {fileName}.csv")
    # else
    with open(f"{fileName}.csv", "w+") as f:
        f.write(df.to_csv())
    with open(f"{fileName}.txt", "w+") as f:
        f.write(df.to_string())
