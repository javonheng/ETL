# imports
from utils.log import log
from utils.email import send_email, draft_email_body
from utils.snowflake_connection import snowflake_conn
import snowflake.connector
import pandas as pd
import time

logfile_name = 'insert_bitcoin_csv'

################## EXTRACT from excel #######################
log(logfile_name, "INFO", 'Extract Phase Started ...')
bitcoin_csv = pd.read_csv('./excel/coin_Bitcoin.csv')
log(logfile_name, "INFO", 'Extract Phase Ended ...')


################# TRANSFORM #######################
log(logfile_name, "INFO", 'Transform Phase Started...')
## add new column average
bitcoin_csv['Average'] = (bitcoin_csv['High'] + bitcoin_csv['Low']) / 2

## drop column volume and marketcap axis x = 0, y = 1
bitcoin_csv = bitcoin_csv.drop(['Volume', 'Marketcap'], axis=1)

## round off prices to 2 dp
bitcoin_csv = bitcoin_csv.round(decimals=2)

## convert datetime to only date + stringify for insert
bitcoin_csv['Date'] = pd.to_datetime(bitcoin_csv["Date"], format="%Y/%m/%d %H:%M:%S")
bitcoin_csv['Date'] = bitcoin_csv['Date'].astype(str)

# print(bitcoin_csv.head())
log(logfile_name, "INFO", 'Transform Phase Ended...')


################## LOAD to DB #################
log(logfile_name, "INFO", 'Load Phase Started...')
## get all column names of df
column_name = list(bitcoin_csv.columns)
column_name.remove('SNo')

## convert dataframe to array
bitcoin_csv_list = bitcoin_csv.values.tolist()
for item in bitcoin_csv_list:
    item.pop(0)

columns = "(%s)" % (", ".join([str(elem).upper() for elem in column_name]))

query_string = 'INSERT INTO CRYPTO_HIST_PRICES ' + columns + ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
err_count = 0
conn = snowflake_conn()
cursor = conn.cursor()
cursor.fast_executemany = True

start_time = time.time()
try:
    cursor.executemany(query_string, bitcoin_csv_list)
    count = cursor.rowcount
    conn.commit()
except snowflake.connector.errors.ProgrammingError as e:
    # default error message
    print(e)
    # customer error message
    error_message = 'Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
    log(logfile_name, "ERROR", error_message)
    conn.rollback()


cursor.close()
conn.close()
## time taken to complete transaction in milliseconds
time_taken = (time.time() - start_time) * 1000
time_taken_round = round(time_taken, 3)

############## POST LOAD - send email and log out results ################

# rows_inserted = cursor.rowcount
log(logfile_name, "INFO", str(count) + ' rows inserted.')
if err_count > 0:
    log(logfile_name, "ERROR", str(err_count) + ' rows with error.')

# cursor.close()
log(logfile_name, "INFO", 'Load Phase Ended...')
send_email(logfile_name, 'Bitcoin CSV Inserted Into DB', draft_email_body(str(count), str(err_count), str(time_taken_round)), [])


