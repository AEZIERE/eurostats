import math
import time
from enum import Enum

import pandas as pd

from config import bdd_connection, engine_conn

df_meta = pd.read_csv("../File csv eurostats/csv//search_for_insert/metadata.csv", sep=";")
df_time = pd.read_csv("../File csv eurostats/csv//search_for_insert/temporel.csv", sep=";")
class enum_freq(Enum):
    A = "annee"
    S = "semestre"
    Q = "trimestre"
    M = "mois"




def select_id_meta(data_flow, last_update, freq, unit):
    with bdd_connection() as conn:
        cur = conn.cursor()

        select_id_meta = "SELECT id FROM public.metadata WHERE data_flow = %s AND last_update = %s AND freq = %s AND code_unit = %s"
        cur.execute(select_id_meta, (data_flow, last_update, freq, unit))
        db_row = cur.fetchone()

        return db_row
def select_id_time(freq, string):
    with bdd_connection() as conn:
        cur = conn.cursor()

        name_freq = enum_freq[freq].value
        annee, other = string.split("-")
        name_id = "id_" + name_freq
        if 'Q' in other:
            other = other.replace('Q', '')

        select_id_time = f"""SELECT {name_id} FROM public.temporel WHERE annee = %s AND {name_freq} = %s"""
        cur.execute(select_id_time, (annee, other))

        db_row = cur.fetchone()

        return db_row

# search in file csv for boost time of insert
def search_id_meta_csv(data_flow, last_update, freq, unit):
    #df = pd.read_csv("csv/search_for_insert/metadata.csv", sep=";")
    if unit == "":
        df= df_meta[(df_meta['data_flow'] == data_flow) & (df_meta['last_update'] == last_update) & (df_meta['freq'] == freq)]
    else:
        df = df_meta[(df_meta['data_flow'] == data_flow) & (df_meta['last_update'] == last_update) & (df_meta['freq'] == freq) & (
                df_meta['code_unit'] == unit)]
    if df.empty:
        return None
    else:
        return df['id'].values[0]
def search_id_time_csv(freq, string):
    #df = pd.read_csv("csv/search_for_insert/temporel.csv", sep=";")

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


def return_id(conn, cur, row):
    id_meta = select_id_meta(row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit'])
    id_time = select_id_time(row['freq'], row['TIME_PERIOD'])

    if id_meta is None:
        cur.execute(insert_data(name_table="metadata", data_flow=row['DATAFLOW'], last_update=row['LAST UPDATE'],
                                freq=row['freq'], code_unit=row['unit']))
        conn.commit()
        id_meta = select_id_meta(row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit'])

    if id_time is None:
        id_time = "XXXX"
    id_meta = id_meta[0]
    id_time = id_time[0]
    return id_meta, id_time


def add_row_csv(name, data):
    df = pd.read_csv("csv/search_for_insert/" + name + ".csv")
    last_id = df.iloc[-1]['id']
    data.insert(0, last_id + 1)
    df.loc[len(df.index)] = data
    df.to_csv("csv/search_for_insert/" + name + ".csv", index=False)


def requete_insert_all(tab):
    argument_string = ",".join(
        "('%s', '%s', '%s', '%s', '%s', '%s')" % (a, z, e, r, t, y) for (a, z, e, r, t, y) in tab)
    return argument_string


def global_insert_all(conn, cur, row):
    id_meta, id_time = return_id(conn, cur, row)

    # test value is not nan
    if row['OBS_VALUE'] == "nan":
        row['OBS_VALUE'] = 0
    if math.isnan(row['OBS_VALUE']):
        row['OBS_VALUE'] = 0

    return (id_meta, id_time, row['indic'], row['nace_r2'], row['geo'], row['OBS_VALUE'])
def insert_dataByCol(**kwargs):
    """create string of data to insert in table
    :param kwargs: name_table, columns, values
    :return: insert : string

    """
    kwargs_filtered = {k: "'" + v + "'" if isinstance(v, str) else v for k, v in kwargs.items() if
                       k != 'name_table'}
    liste_col, liste_val = zip(*kwargs_filtered.items())

    insert = """INSERT INTO public.{table} ({columns}) VALUES ({values})""".format(
        table=kwargs['name_table'],
        columns=", ".join(liste_col),
        values=", ".join(str(val) for val in liste_val)
    )
    print(insert)
    return insert
def insert_all(conn, cur, row):
    id_meta, id_time = return_id(conn, cur, row)
    # test value is not nan
    if row['OBS_VALUE'] == "nan":
        row['OBS_VALUE'] = 0
    if math.isnan(row['OBS_VALUE']):
        row['OBS_VALUE'] = 0

    cur.execute(insert_data(name_table="secteur_industrie", id_meta=id_meta, id_time=id_time, code_indic=row['indic'],
                            code_nace_r2=row['nace_r2'], code_geo=row['geo'], value=row['OBS_VALUE']))

    conn.commit()


def insert_data_csv(row):
    if "unit" not in row:
        row['unit'] = ""
    id_meta = search_id_meta_csv(row['DATAFLOW'], row['LAST UPDATE'], row['freq'], row['unit'])
    if row['freq'] != "A":
        print("probleme")
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
    #print(id_meta, id_time, row['indic_ag'], row['itm_newa'], row['geo'], row['OBS_VALUE'])
    return (id_meta, id_time, row['nace_r2'], row['s_adj'], row['na_item'], row['geo'], row['OBS_VALUE'])

def secteur_industrie_engine():
    path = "../File csv eurostats/csv/all/"
    df = pd.read_csv(path + 'namq_10_a64_e_linear.csv')
    #tab = ["NSA", "SCA"]
    #df = df[df['s_adj'].isin(tab)]
    print(len(df.index))
    time_start = time.time()

    with engine_conn() as conn:
        # Methode 3
        for i in range(0, len(df.index), 50000):
            time_start_loop = time.time()
            df_insert = df.iloc[i:i + 50000].apply(lambda row: pd.Series(insert_data_csv(row)), axis=1)
            #df_insert = df.apply(lambda row: pd.Series(insert_data_csv(row)), axis=1)
            df_insert.columns = ['id_meta', 'id_annee', "code_nace_r2", "code_s_adj", "code_na_item",'code_geo', 'value']
            df_insert.to_sql('agriculture', conn, if_exists='append', index=False)
            conn.commit()
            print("time : ", time.time() - time_start_loop)

        print("time : ", time.time() - time_start)


def secteur_industrie():
    path = "../File csv eurostats/csv/all/"
    df = pd.read_csv(path + 'namq_10_a10_linear.csv')
    tab = ["NSA", "SCA"]
    df = df[df['s_adj'].isin(tab)]

    time_start = time.time()
    with bdd_connection() as conn:
        cur = conn.cursor()
        # Methode 1
        # [insert_all(conn, cur, row) for index, row in df.iterrows() ]

        # Methode 2
        print(len(df.index))
        tab_rows = [insert_data_csv(index, row) for index, row in df.iterrows()]

        #df = pd.DataFrame(tab_rows)
        #df.to_csv("csv/search_for_insert/secteur_industrie.csv", index=False, header=False)
        #print("save csv")

        argument_string = requete_insert_all(tab_rows)
        cur.execute("INSERT INTO {table} (id_meta, id_trimestre, code_s_adj, code_nace_r2, code_na_item, code_geo, value  ) VALUES".format(table="secteur_industrie") + argument_string)
        conn.commit()

    print("done {ecart} seconde".format(ecart=time.time() - time_start))


if __name__ == '__main__':
    secteur_industrie_engine()
