import time
from pathlib import Path
import traceback
import psycopg2
import schedule
import numpy as np
import pandas as pd
import algorithm
import clean_fish
import data_clean
import emailing
import exception_handling
import postgres
import configparser

def timestamp(msg):
    # define now as 12am EST
    now = np.datetime64('now') - np.timedelta64(1, 'h')
    print(now,  msg)

def main(host, db, user, password, email, first_run):

    if first_run:
        subject = 'opened'
        body = "I'm up!!"
        emailing.ping(subject, body)

    # get yesterday's date
    date = str(np.datetime64('today') - np.timedelta64(1, 'D'))
    timestamp("checking for outliers...")

    try:
        # get today's records and clean
        pg_data = postgres.query_data(host, db, user, password, date)
        if pg_data.shape[0] == 0:
            timestamp("There was no data yesterday!")
            timestamp("I'm done for today. Zzzz.....")
            if first_run:
                return schedule.CancelJob
            else:
                return True
        data = postgres.clean_postgres_data(pg_data)
        # we'll use a 'lite' version of `data` called `df`
        df = data_clean.main(data)

        # load up existing dataset
        archive_path = Path('./data/pg_data_clean.csv')
        archive_df_path = Path('./data/clean_catch_data.csv')

        try:
            archive = pd.read_csv(str(archive_path))
            archive_df = pd.read_csv(str(archive_df_path))
            # update our existing clean datasets with today's data
            # without changing the data we just extracted
            archive.append(data).to_csv(archive_path, index=False)
            archive_df.append(df).to_csv(archive_df_path, index=False)
        # in case its the first run or file was deleted:
        except FileNotFoundError:
            pg_archive = postgres.query_data(host, db, user, password)
            archive = postgres.clean_postgres_data(pg_archive)
            archive.to_csv(archive_path, index=False)
            archive_df = data_clean.main(archive)
            archive_df.to_csv(archive_df_path, index=False)

        # load fish data; eventually set this up like catch data where
        # the pg server is queried and the raw data is cleaned
        fishpath = Path('./data/fishdata_buyingunit_clean.csv')
        fish = pd.read_csv(fishpath)
        countries = df['country'].unique()

        # flagged will hold records flagged as potential outliers, from which we will
        # use the id's to pull from `data` for full context
        flagged = pd.DataFrame()
        # images is a dict as follows {plot_num: fpath}
        # to be used for attaching plots to emails
        images = []

        for country in countries:
            c_flagged, c_images = algorithm.main(df, archive_df, fish, country, date)
            if c_flagged.shape[0] > 0:
                flagged = flagged.append(c_flagged)
                images.extend(c_images)

        if flagged.shape[0] > 0: # if any samples were flagged
            timestamp("I found something fishy in yesterday's data!")
            flagged_data = data[data['id'].isin(flagged['id'])]
            num_flagged = flagged_data.shape[0]
            flagged_fname = date+'.csv' # change this eventually
            flagged_path = Path("./flagged_data/"+flagged_fname)
            flagged_data.to_csv(str(flagged_path), index=False)
            emailing.email_results(email, num_flagged, flagged_path, flagged_fname, images)
        else:
            timestamp("I found nothing fishy in yesterday's data!")

        subject = 'daily ping'
        body = 'still alive!'
        emailing.ping(subject, body) # daily check if code is live or not

    # in case something goes wrong anywhere in the program, send error log and quit:
    except Exception as e:
        logpath = Path('./logs/'+date+'.log')
        with open(logpath, 'w') as logf:
            traceback.print_exc(file=logf)
        exception_handling.send_error_log(logpath)
        exception_handling.print_error_message()
        quit()

    timestamp("I'm done for today. Zzzz.....")

    if first_run:
        return schedule.CancelJob

# for the beginning of the program, initialize things like postgres and email info

login_errors = (psycopg2.errors.InFailedSqlTransaction,
                psycopg2.OperationalError)
# for some reason, a wrong password will not cause a problem,
# data can be queried just fine... not a problem for now I guess

cfg = configparser.ConfigParser()
cfg.read('config.ini')
host = cfg.get('postgres', 'host_address')
db = cfg.get('postgres', 'db_name')
user = cfg.get('postgres', 'user')
password = cfg.get('postgres', 'password')

try:
    # test ping the postgres server
    postgres.query_data(host, db, user, password, 'test')
except login_errors:
    # config.ini info is wrong, prompt user until we have correct info
    exception_handling.print_error_message('login')
    host = None
    db = None
    user = None
    password = None
    while (host is None) or (db is None) or (user is None) or (password is None):
        host, db, user, password = postgres.login()
        try:
            postgres.query_data(host, db, user, password, 'test')
        except login_errors:
            exception_handling.print_error_message('login')
            host = None
            db = None
            user = None
            password = None

# get email address
email = cfg.read('email', 'user_email')
while email is None:
    window_title = "Email",
    prompt = "Please enter the email address where you would like notifications to go to."
    email = emailing.ask_email(window_title, prompt)

# scan for outliers in all data up til now as part of the first run
schedule.every().second.do(main, host, db, user, password, email, True)

# now just scan for outliers once a day
schedule.every().day.at("00:00").do(main, host, db, user, password, email, False)

while True:
    schedule.run_pending()
    time.sleep(1)
