import math
import time
from enum import Enum

import pandas as pd

from config import bdd_connection, engine_conn

df_meta = pd.read_csv("../File csv eurostats/csv/search_for_insert/metadata.csv", sep=",")
df_time = pd.read_csv("../File csv eurostats/csv/search_for_insert/temporel.csv", sep=";")


class enum_freq(Enum):
    A = "annee"
    S = "semestre"
    Q = "trimestre"
    M = "mois"


def create_table(name_table, columns):
    with bdd_connection() as conn:
        cur = conn.cursor()

        create_table = f"""CREATE TABLE IF NOT EXISTS public.{name_table} ()"""
        cur.execute(create_table)
        conn.commit()
        # alter table
        alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY"""
        cur.execute(alter_table)
        conn.commit()
        for column in columns:
            alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS {column} varchar(255)"""
            cur.execute(alter_table)
            conn.commit()


# search in file csv for boost time of insert
def search_id_meta_csv(data_flow, last_update, freq, unit):
    if unit == "":
        df = df_meta[
            (df_meta['data_flow'] == data_flow) & (df_meta['last_update'] == last_update) & (df_meta['freq'] == freq)]
    else:
        df = df_meta[(df_meta['data_flow'] == data_flow) & (df_meta['last_update'] == last_update) & (
                df_meta['freq'] == freq) & (
                             df_meta['code_unit'] == unit)]
    if df.empty:
        return None
    else:
        return df['id'].values[0]


def search_id_time_csv(freq, string):
    # df = pd.read_csv("csv/search_for_insert/temporel.csv", sep=";")

    name_freq = enum_freq[freq].value
    if name_freq == "annee":
        df = df_time[(df_time['annee'] == int(string))]
        return df["id_annee"].values[0]
    else:
        annee, other = string.split("-")
        name_id = "id_" + name_freq
        if 'Q' in other:
            other = other.replace('Q', '')
        if len(other) > 1:
            other = other.replace('0', '')
        df = df_time[(df_time['annee'] == int(annee)) & (df_time[str(name_freq)] == int(other))]
        if df.empty:
            return None
        else:
            return df[name_id].values[0]


def add_row_csv(name, data):
    df = pd.read_csv("../File csv eurostats/csv/search_for_insert/" + name + ".csv", sep=",")
    last_id = df['id'].iloc[-1]
    data.insert(0, last_id + 1)
    df.loc[len(df.index)] = data

    df.to_csv("../File csv eurostats/csv/search_for_insert/" + name + ".csv", index=False)

    global df_meta
    df_meta = pd.read_csv("../File csv eurostats/csv/search_for_insert/metadata.csv", sep=",")


# *--------------------------*#

# Functions return row to insert in table

# *--------------------------*#


def insert_data_csv(row, columns):
    if "unit" not in row:
        row['unit'] = ""
    id_meta = search_id_meta_csv(row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit'])
    id_time = search_id_time_csv(row['freq'], row['TIME_PERIOD'])

    if id_meta is None:
        add_row_csv("metadata", [row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit']])
        id_meta = search_id_meta_csv(row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit'])

    if id_time is None:
        id_time = "XXXX"

    # test value is not nan
    if row['OBS_VALUE'] == "nan":
        row['OBS_VALUE'] = 0
    if math.isnan(row['OBS_VALUE']):
        row['OBS_VALUE'] = 0
    if row['OBS_VALUE'] == '':
        row['OBS_VALUE'] = 0
    # print(id_meta, id_time, row['indic_ag'], row['itm_newa'], row['geo'], row['OBS_VALUE'])

    # make tuple with columns
    tuple = ()
    for column in columns:
        if column == "id_meta":
            tuple = tuple + (id_meta,)
            continue
        if column == "id_annee" or column == "id_semestre" or column == "id_trimestre" or column == "id_mois":
            tuple = tuple + (id_time,)
            continue
        if column == "value":
            tuple = tuple + (row['OBS_VALUE'],)
            continue
        column = column.replace("code_", "")
        tuple = tuple + (row[column],)

    return tuple


# *--------------------------*#

# Functions main read, sort, insert data in table

# *--------------------------*#
def main_insert_engine(name_file, name_table, columns):
    path = "../File csv eurostats/csv/all/"
    df = pd.read_csv(path + name_file + '_linear.csv')

    # Trie des données
    # tab = ["NSA", "SA"]
    # df = df[df['s_adj'].isin(tab)]

    print(len(df.index))

    time_start = time.time()

    #init columns


    # create table and columns

    create_table(name_table, columns)

    with engine_conn() as conn:
        # Methode 3
        for i in range(0, len(df.index), 50000):
            time_start_loop = time.time()

            df_insert = df.iloc[i:i + 50000].apply(lambda row: pd.Series(insert_data_csv(row, columns)), axis=1)
            # df_insert = df.apply(lambda row: pd.Series(insert_data_csv(row)), axis=1)
            df_insert.columns = columns

            df_insert.to_sql(name_table, conn, if_exists='append', index=False)
            conn.commit()
            print("time : ", time.time() - time_start_loop)

        print("time end : ", time.time() - time_start)


if __name__ == '__main__':
    name_file = 'educ_uoe_enra11'
    name_table = "education"
    columns = ["id_meta", "id_annee", "code_isced11", "code_sex", "code_geo", "value"]
    main_insert_engine(name_file, name_table, columns)
