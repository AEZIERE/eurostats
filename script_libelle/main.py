import os

import pandas as pd


if __name__ == '__main__':

    # read the path
    file_path = "libelle/"
    # list all the files from the directory
    file_list = os.listdir(file_path)
    df = pd.DataFrame()
    for file in file_list:
        print(file_path + file)

        df_temp = pd.read_csv(file_path + file, sep='\t', header=None, names=['code','libelle'])
        df = pd.concat([df, df_temp], axis=0)

    print(len(df.index))
    df.to_csv('out.csv', index=False)

