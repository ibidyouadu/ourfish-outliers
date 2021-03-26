import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns; sns.set()
pd.options.mode.chained_assignment = None
from scipy.spatial.distance import mahalanobis
from pathlib import Path

def iqr_method(f_df, col):
    """
    Helper function. Flag potential outliers using conventional 1D IQR rule
    """
    x = f_df[col]
    q1 = np.quantile(x, 0.25)
    q3 = np.quantile(x, 0.75)
    iqr = q3 - q1
    far = f_df[(x > q3 + 1.5*iqr) | (x < q1 - 1.5*iqr)]
    return far
    
def mahalanobis_method(f_df, expl_vars, mu, country, has_limits):
    """
    Helper function. Flag potential outliers if they exceed the 90th percentile
    Mahalanobis distance by a certain threshold (which varies by country)
    """
    cov = f_df[expl_vars].cov()
    vi = np.linalg.inv(cov)
    m_dist = []
    
    for ii, row in f_df[expl_vars].iterrows():
        d = mahalanobis(mu, row, vi)
        m_dist.append(d)
    
    q90 = np.quantile(m_dist, 0.9)
    if has_limits:
        fence_factor = {
            'HND': 2,
            'IDN': 1.5,
            'MOZ': 2, # currently unused
            'PHL': 2
        }
    else:
        fence_factor = {
            'HND': 1.5,
            'IDN': 2,
            'MOZ': 2,
            'PHL': 2.5
        }
    fence = fence_factor[country]*q90
    far = f_df[m_dist > fence]
    return far

def far_enough(obs, expl_vars, mu):
    """
    Remove points from oob/far that are too close to the mean. Too close means
    Less than 0.5 units in the x-directions and less than 1 unit in the
    y-direction, both in log-scale
    """
    x = expl_vars[0]
    y = expl_vars[1]
    return obs[(np.abs(obs[x] - mu[0]) > 0.5) | \
                (np.abs(obs[y] - mu[1]) > 1)]
                
def plot_data(f, f_df, ycol, mu, far, oob, limits):
    """
    Plot samples and mark points that are flagged by distance and exceeding limits.
    
    Parameters
    ----------
    f (DataFrame)
        fish information from clean_fish.py. Used for veryfying weight_units
    f_df (DataFrame)
        samples for the current fish, taken from df from data_clean.py
    ycol (str)
        either weight_kg or weight_lbs (todo:count)
    mu (Series)
        centroid of samples
    far (DataFrame)
        samples that exceed Mahalanobis fence
    oob (DataFrame)
        samples that exceed pre-programmed thresholds, if any
    limits (dict[float])
        thresholds from f
        
    
    Returns:
    ax (AxesSubplot)
        Plot object for saving later.
    """
    ax = sns.scatterplot(data=f_df, x='unit_price', y=ycol)

    # plot the mean point
    ax.scatter(x=mu[0], y=mu[1],
           color='r', marker='*', s=200)

    # mark m_dist far points with a magenta +
    ax.scatter(x=far['unit_price'],
           y=far[ycol],
           s=100, color='magenta', marker='+')
           
    # plot stuff relating to the thresholds

    # mark points exceeding thresholds with a green x
    ax.scatter(x=oob['unit_price'],
               y=oob[ycol],
               s=100, color='green', marker='x')

    # highlight the regions exceeding thresholds
    x_min = ax.get_xlim()[0]
    x_max = ax.get_xlim()[1]
    y_min = ax.get_ylim()[0]
    y_max = ax.get_ylim()[1]

    # exceeding price min
    if x_min < limits['price_min']:
        ax.axvspan(xmin=x_min, xmax=limits['price_min'],
                   facecolor='r', alpha=0.1)
    else:
        ax.plot(limits['price_min']*np.ones(2), [y_min, y_max], 'r')

    # exceeding price max
    if limits['price_max'] < x_max:
        ax.axvspan(xmin=limits['price_max'], xmax=x_max,
                   facecolor='r', alpha=0.1)
    else:
        ax.plot(limits['price_max']*np.ones(2), [y_min, y_max], 'r')
    
    # exceeding weight max
    if limits['weight'] < y_max:
        ax.axhspan(ymin=limits['weight'], ymax=y_max,
                  facecolor='b', alpha=0.1)
    else:
        ax.plot([x_min, x_max], limits['weight']*np.ones(2), 'b')

    plt.close()
    
    return ax
    
def main(df, archive_df, fish, country, date):
    """
    Flag potential outliers in the dataset obtained from psql server.
    The algorithm works as follows:
    
    1. Find the subset of unique buying_unit from df that exist in fish.
    2. Filter out the fish that have less than 10 samples.
    3. Iterate through these fish and do as follows:
        a. if the distribution of the explanatory variables looks like a straight
            line, do a simple 1D IQR method to find "far" points (far)
        b. otherwise, use mahalanobis distance to find "far" points (far)
        c. find points (oob) that exceed thresholds from the fish db
        d. flag points (flagged) that belong to both sets described in a/b and c
        e. create and save plots for any fish species w/ flagged points
    
    One issue with this algorithm is how to iterate through the data. Currently,
    we iterate through the fish name, buying_unit. About 1400 samples (as of
    2021-03-12) have this as nan, so they are discarded. The alternative is to
    use the buying_unit_id, but the same fish species can have different
    buying_unit_id, so this makes our sets incomplete. An attempt to map
    nan's  to a buying_unit by using the buying_unit_id failed because when you
    join df and fish on df.buying_unit_id/fish.id, the species name is not
    necessarily going to match! On the plus side, almost all the samples that
    have nan buying_unit are from 2019. Only 2 are from 2020, and none from 2021
    (so far!). So as of now, this method works best.
    
    Parameters
    ----------
    df (DataFrame)
        Today's transaction samples.
    archive_df (DataFrame)
        All transaction samples.
    fish (DataFrame)
        Cleaned version of fish dataset
    country (str)
        Country code e.g. 'HND'
    
    Returns:
    flagged (`pd.DataFrame`)
        Subset of df containing potential outliers.
    """
    c_df = df.query("country == @country")
    fish_list = c_df['buying_unit'].unique()
    important_fish = archive_df.query("buying_unit.isin(@fish_list)")\
                        .groupby(by='buying_unit', dropna=True).size()
    important_fish = important_fish[important_fish >= 10].index.values

    # todo: update this to include records that use count instead of weight
    if country == 'HND':
        expl_vars = ['unit_price', 'weight_lbs']
    else:
        expl_vars = ['unit_price', 'weight_kg']
    ycol = expl_vars[1]
    
    # the following are running tallies for results
    samples = 0
    
    images = [] # this will be passed to emailing.py
    flagged = pd.DataFrame()
    
    df_all = archive_df.append(df)
    for fname in important_fish:
        if fname in fish['name'].unique(): # fish has thresholds on record
            has_limits = True
        else:
            has_limits = False
            
        f = fish.query("name == @fname")
        f_df = df_all.query("buying_unit == @fname")
        # add 1e-1 to avoid log(0)
        f_df[expl_vars] = f_df[expl_vars].apply(lambda x: np.log10(x+1e-1))
        samples += f_df.shape[0]
 
        mu = f_df[expl_vars].mean() # used for m_dist and plotting
        if f_df['unit_price'].var() == 0: # observations are 1D in unit_price-weight
            far = iqr_method(f_df, ycol)

        elif f_df[ycol].var() == 0: # same but horizontally
            far = iqr_method(f_df, 'unit_price')
    
        else: # do mahalanobis distance method
            far = mahalanobis_method(f_df, expl_vars, mu, country, has_limits)
        far = far[far['id'].isin(df['id'])] # only take today's samples
        
        # assign fences for pre-programmed thresholds
        limits = {
            'weight': np.nan,
            'price_min': np.nan,
            'price_max': np.nan
        }
        
        if has_limits: # if fish has thresholds on record
            # get the threshold values from fish
            # indexing at 0 to avoid VisibleDeprecationWarning
            limits['weight'] = np.log10(f['weight_max'].values[0]+1e-1) 
            limits['price_min'] = np.log10(f['price_min'].values[0]+1e-1)
            limits['price_max'] = np.log10(f['price_max'].values[0]+1e-1)
        
        # in case there are any threshold records missing/the fish had
        # non on record, define the thresholds a certain distance away
        # from the centroid
        for k in limits.keys():
            if np.isnan(limits[k]): # did not have existing limit
                if k == 'weight':
                    limits[k] = mu[1] + 2
                elif k == 'price_min':
                    limits[k] = mu[0] - 1.5
                elif k == 'price_max':
                    limits[k] = mu[0] + 1.5

        # find samples that exceed at least one threshold
        if (f['weight_units'] == 'lbs').any():
            oob = f_df.query("(weight_lbs > @limits['weight']) | \
                                (unit_price < @limits['price_min']) | \
                                (unit_price > @limits['price_max'])")
            oob = oob[oob['id'].isin(df['id'])] # only take today's samples
        else:
            oob = f_df.query("(weight_kg > @limits['weight']) | \
                                (unit_price < @limits['price_min']) | \
                                (unit_price > @limits['price_max'])")
            oob = oob[oob['id'].isin(df['id'])]
    
        # potential outliers for this fish are both oob and far
        oob = far_enough(oob, expl_vars, mu)
        far = far_enough(far, expl_vars, mu)
        flag_ids = pd.merge(oob, far, on='id', how='inner')['id'].unique()
        
        
        # record flagged samples, create scatter plot and save
        if flag_ids.size > 0: # only if any samples were flagged
            flagged = flagged.append(df.query("id.isin(@flag_ids)"))
            ax = plot_data(f, f_df, expl_vars[1], mu, far, oob, limits)
                
            ax.set_title("country="+country+", buying_unit="+str(fname)+\
                        "\n %d potential outlier(s) (n=%d)"\
                        % (flag_ids.size, f_df.shape[0]))
            
            date = date.replace('-','_')
            plot_num = len(images)+1
            img_path = Path('./plots/'+date+'/plot%.2d.png' % plot_num)
            images.append(img_path)
            img_path.parent.mkdir(exist_ok=True) # create dir for the date
            ax.figure.savefig(img_path, dpi=150)
    return flagged, images