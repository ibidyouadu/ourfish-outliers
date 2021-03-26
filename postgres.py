import psycopg2
from pathlib import Path
import tkinter as tk
import tkinter.simpledialog as simpledialog
import ast
import pandas as pd
import numpy as np
from clean_fish import fix_weight_units

def prompt_user(window_title, prompt, for_password=False):
    """
    Helper function for prompting user login info
    """
    root = tk.Tk()
    root.withdraw()
    if for_password:
        inp = simpledialog.askstring(window_title, prompt, show='*', parent=root)
    else:
        inp = simpledialog.askstring(window_title, prompt, parent=root)
    root.destroy()
    
    return inp

def login():
    """
    Prompt user for pg login info using tkinter gui
    
    Returns:
    user (str)
        pg user name to use to connect to pg server
    password (str)
        pg password
    """
    host = prompt_user("Postgres Login",
                        "Please enter the database host address")

    db = prompt_user("Postgres login",
                        "Please enter the name of the database. (e.g. ourfish)")
                        
    user = prompt_user("Postgres Login",
                        "Please enter your user name.")

    password = prompt_user("Postgres Login",
                            "Please enter your password.",
                            for_password=True)

    return host, db, user, password

def query_data(host, db, user, password, date=None):
    """
    query yesterday's catch data and write it out to a csv
    """
    conn = psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=password)
    
    cur = conn.cursor()
    if date is None: # if there is no archived data already
        date = '2019-01-01'
        sql = """SELECT * FROM fishdata_catch
        WHERE date::date >= \'{}\'""".format(date)
    elif date=='test': # checking user login info
        sql = "SELECT * FROM fishdata_catch LIMIT 1"
        cur.execute(sql) # this will fail if login info is wrong
        cur.close()
        conn.close()
        return None
    else:
        sql = """SELECT * FROM fishdata_catch
        WHERE date::date = \'{}\'""".format(date)
    copy_sql = "COPY ("+sql+") TO STDOUT WITH CSV HEADER"
    csv_path = Path('./data/postgres_dump.csv')
    
    
    with open(str(csv_path), 'w') as f:
        cur.copy_expert(copy_sql, f)
    
    cur.close()
    conn.close()
    
    return pd.read_csv(str(csv_path))

def unravel(row_data):
    """
    Helper function for clean_postgres_data.
    Parses the `data` column and returns a more usable format for further cleaning:
    str that looks like a dict -> list of lists (one for each column)
    The process looks very... clunky. but it JUST WORKS!
    """
    row_data = row_data.replace('{', '').replace('}', '').split(', "')
    for ii in range(len(row_data)):
        row_data[ii] = row_data[ii].split(': ')
        row_data[ii][0] = row_data[ii][0].replace('"', '')
        
        val = row_data[ii][1]
        if val == '""':
            val = np.nan
        elif val.lower() == 'true':
            val = True
        elif val.lower() == 'false':
            val = False
        else:
            val = ast.literal_eval(val)
        row_data[ii][1] = val
    
    return row_data

def clean_postgres_data(pg_data):
    """
    Clean the data exported from query_data. In particular, unravel all the information
    in the `data` column which contains the info we need; the raw postgres data has just
    6 columns: id, date, data, buyer_id, buyer_unit_id, and fisher_id
    Where is weight_kg, unit_price, buying_unit etc?? It's all in `data`. So the goal
    of this method is to unpack the `data` column into many (20) columns of information
    that it contains.
    """
    pg_data['data'] = pg_data['data'].apply(unravel)
    
    cols = [record[0] for record in pg_data.iloc[0]['data']]
    # this will contain lists that will turn into new columns of pg_data
    data_dict = {}
    
    for col in cols:
        data_dict[col] = []

    for ii, row in pg_data.iterrows():
        data = row['data']
        keys = [record[0] for record in data]
        vals = [record[1] for record in data]
        row_dict = {k: v for (k,v) in zip(keys, vals)}
    
        for col in cols:
            data_dict[col].append(row_dict[col])
    
    for col in cols:
        pg_data[col] = data_dict[col]
        
    pg_data = pg_data.drop(columns='data') # no longer need that poorly formatted col
    pg_data = pg_data.rename(mapper={'name':'buying_unit'}, axis=1)
    
    # use currencies to create country col
    country_currency_dict = {
        'IDR': 'IDN',
        'PHP': 'PHL',
        'MOP': 'MOZ',
        'MZN': 'MOZ',
        'HNL': 'HND'
    }

    # filter out countries outside of the main 4
    pg_data = pg_data[pg_data['price_currency'].isin(country_currency_dict.keys())]

    # create country column
    pg_data['country'] = pg_data['price_currency'].apply(lambda x: country_currency_dict[x])
    
    # fix weight_units in the same manner that is done in clean_fish.py
    pg_data['weight_units'] = pg_data['weight_units'].apply(fix_weight_units)

    # create weight_kg and weight_lbs cols
    # do this by creating two arrays that function as a scalar factor at every row
    # the arrays know whether to convert a measurement to kg or to keep it in lbs
    # and vice versa
    kg_conv = []
    lbs_conv = []
    for ii, row in pg_data.iterrows():
        if row['weight_units'] == 'kg':
            kg_conv.append(1)
            lbs_conv.append(2.205)
        else:
            lbs_conv.append(1)
            kg_conv.append(1/2.205)

    kg_conv = np.array(kg_conv)
    lbs_conv = np.array(lbs_conv)
    pg_data['weight_kg'] = pg_data['weight']*kg_conv
    pg_data['weight_lbs'] = pg_data['weight']*lbs_conv
    
    pg_data = pg_data[~pg_data.duplicated()]

    return pg_data