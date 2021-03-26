import pandas as pd
import numpy as np
from pathlib import Path

def fix_weight_units(wu):
    """
    Helper function to fix strings in weight_units column of fishdata_buyingunit.csv
    The weight_units column has variations of kg, lbs, plus some nans.
    fix_weight_units will do the following map:
    
    kg/Kg/nan -> kg
    lbs/Lbs/Ib/Ibs -> lbs
    
    nan is converted to kg because (1) most weight_units are kg anyway and
    (2) if a record has weight_kg > weight_max (lbs), then it is definitely oob
    
    Parameters
    ----------
    wu (str or nan)
        the string in the weight_units column
    
    Returns:
    wu (str)
        either 'kg' or 'lbs'
    """
    if type(wu) == float: # nans
        wu = 'kg'
    else:
        wu = wu.lower()
        if wu in ['lb', 'ib', 'ibs']:
            wu = 'lbs'
    return wu

def main():
    """
    Clean and condense fishdata_buyingunit.csv
    
    (1) extract only the necessary columns
    (2) drop rows which have only nans in the numeric columns
    (3) fix the strings in the weight_units column
    (4) remove duplicate rows
    (5) condense rows with the same fish name to one row
    (6) construct df from (5)
    
    Parameters
    ----------
    df (DataFrame)
        All the samples obtained from data_clean
    
    Returns:
    fish (DataFrame)
        Each row uniquely identifies the species of fish from the
        fishdata_buyingunit.csv dataset.
    """
    # (1)
    fish_cols = ['name', 'id', 'weight_units', 'count_max',\
                    'weight_max', 'price_min', 'price_max']
    fish_path = Path("./data/fishdata_buyingunit.csv")
    fish = pd.read_csv(fish_path)[fish_cols]
    # (2)
    # (3)
    fish['weight_units'] = fish['weight_units'].apply(fix_weight_units)
    # (4)
    fish = fish[~fish.duplicated()] # some rows are duplicates
    
    # the following will be used to construct a new df to replace fish
    fish_name_list = fish['name'].unique()
    fish_id_list = []
    weight_units_list = []
    count_max_list = []
    weight_max_list = []
    price_min_list = []
    price_max_list = []

    # (5)
    # in the following loop, we will go through each fish species and condense
    # all samples with the same name to one row. to do this we will have to converge
    # on a single value for each of count_max, weight_units, weight_max,
    # price_min, and price_max
    for fname in fish_name_list:
        f = fish.query("name == @fname")
        
        fish_id_list.append(f['id'])
        
        count_max = f['count_max'].max()
        count_max_list.append(count_max)

        if f['weight_units'].unique().size > 1: # weight_units col has BOTH lbs and kg
            weight_units = 'kg'
            # the following is not a typo; err on the side of larger thresh
            weight_max = f.query("weight_units == 'lbs'")['weight_max'].max() 
            
        else: # the col is either strictly lbs or strictly kg
            weight_units = f.iloc[0]['weight_units']
            weight_max = f['weight_max'].max()
        
        weight_units_list.append(weight_units)
        weight_max_list.append(weight_max)
        
        price_min = f['price_min'].min()
        price_min_list.append(price_min)
        
        price_max = f['price_max'].max()
        price_max_list.append(price_max)

    # (6)
    fish = pd.DataFrame(data={
        'name': fish_name_list,
        'id': fish_id_list,
        'weight_units': weight_units_list,
        'weight_max': weight_max_list,
        'price_min': price_min_list,
        'price_max': price_max_list,
        'count_max': count_max_list
    })
    fishpath = Path('./data/fishdata_buyingunit_clean.csv')
    fish.to_csv(str(fishpath), index=False)
    return fish