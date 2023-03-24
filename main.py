import os
import pandas as pd

if __name__ == '__main__':
    # read the path
    file_path = "../File csv eurostats/csv/all/"
    # list all the files from the directory
    file_list = os.listdir(file_path)
    df = pd.DataFrame()
    object  = {}
    for file in file_list:
        print(file)
        if "DS" in file:
            continue




