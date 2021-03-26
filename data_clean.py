import pandas as pd
import numpy as np

def main(data):
    """
    Clean the samples by removing 'bad' records; ones where it is unclear how
    total_price is calculated (eg count*unit_price vs weight_kg*unit_price)
    to do so, we find records where quantity*unit_price isn't approx total_price
    which it theoretically should be exactly equal to
    """
    cols = ['id','country', 'date',\
    'buyer_id', 'buying_unit', 'buying_unit_id',\
    'count', 'weight_kg', 'weight_lbs', 'unit_price',\
    'total_price']
    df = data[cols]
     
    weight_kg = df['weight_kg']
    weight_lbs = df['weight_lbs']
    count = df['count']
    unit_price = df['unit_price']
    total_price = df['total_price']

    total_price_rel_err_kg = np.abs(weight_kg*unit_price - total_price)/\
                                    (total_price+1e-3)
    total_price_rel_err_lbs = np.abs(weight_lbs*unit_price - total_price)/\
                                    (total_price+1e-3)
    total_price_rel_err_count = np.abs(count*unit_price - total_price)/(total_price+1e-3)
    
    # this will be used to make a new col tracking which units are used 
    # in unit_price eg 'L' means unit_price is in terms of lbs
    price_method = np.empty(df.shape[0], dtype=str)

    errs = {'C': total_price_rel_err_count,
            'L': total_price_rel_err_lbs,
            'K': total_price_rel_err_kg}

    for k in errs.keys():
        price_method[np.where(errs[k] < 0.01)[0]] = k # 1% error tolerance
    df['price_method'] = price_method

    # remove records where quantity*unit_price deviates too much from total_price
    df = df[(total_price_rel_err_kg < 0.01) | \
            (total_price_rel_err_lbs < 0.01) | \
            (total_price_rel_err_count < 0.01)]

    return df