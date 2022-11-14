# imports
from utils.log import log
from utils.email import send_email, draft_email_body
from utils.misc import merge_dict_string, get_time_export_string
from utils.snowflake_connection import snowflake_conn
import snowflake.connector
import datetime
import time
import requests
import pandas as pd
from decouple import config

api_key = config('APILAYER_API_KEY')

# params details
logfile_name = 'insert_currency_rate'
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
previous_day = today - datetime.timedelta(days=2)

base_url = 'https://api.apilayer.com/exchangerates_data'
base = 'SGD'
symbols = 'MYR,KRW,USD,AUD,EUR'
url_fluctuation = base_url + '/fluctuation?base=%s&symbols=%s&start_date=%s&end_date=%s' % (base, symbols, previous_day, yesterday)
url_latest = base_url + '/latest?base=%s&symbols=%s' % (base, symbols)

headers = {
    "apikey": api_key
}

################## EXTRACT from excel #######################
log(logfile_name, "INFO", 'Extract Phase Started ...')
payload = {}

fluctuation_response = requests.request("GET", url_fluctuation, headers=headers, data = payload)
fluctuation_status_code = fluctuation_response.status_code
if fluctuation_status_code != 200:
    log(logfile_name, "ERROR", fluctuation_response.reason)
    exit()
fluctuation_result = fluctuation_response.json()

latest_response = requests.request("GET", url_latest, headers=headers, data = payload)
latest_status_code = latest_response.status_code
if latest_status_code != 200:
    log(logfile_name, "ERROR", latest_response.reason)
    exit()
latest_result = latest_response.json()

# print(status_code, result)
log(logfile_name, "INFO", 'Extract Phase Ended ...')


################# TRANSFORM #######################
log(logfile_name, "INFO", 'Transform Phase Started...')

columns = ['DATE', 'CURRENCY', 'OPEN', 'CLOSE', 'CHANGE', 'CHANGE_PERCENTAGE', "AVERAGE"]
rows = []

## convert timestamp to date
# now = datetime.datetime.now()
# date_formatted = now.strftime("%Y-%m-%d %H:%M:%S")
rates = fluctuation_result['rates']

## iterate through an dict/object
for key in rates:
    # print(key, '->', rates[key])
    rates[key]['average'] = (rates[key]['start_rate'] + rates[key]['end_rate']) / 2

    # round off nested object to 2 dp
    for i, item in rates[key].items():
        rates[key][i] = round(item, 2)

    data = (fluctuation_result['end_date'], key, rates[key]['start_rate'], rates[key]['end_rate'], rates[key]['change'], rates[key]['change_pct'], rates[key]['average'])
    rows.append(data)

log(logfile_name, "INFO", 'Transform Phase Ended...')


################## LOAD to DB #################
log(logfile_name, "INFO", 'Load Phase Started...')

query_string = 'INSERT INTO CURRENCY_RATE (' + ", ".join(columns) + ') VALUES (%s, %s, %s, %s, %s, %s, %s)'
count = 0
err_count = 0
conn = snowflake_conn()
cursor = conn.cursor()
# cursor.fast_executemany = True

start_time = time.time()
failed_data = []

for item in rows:
    try:
        cursor.execute(query_string, item)
        count += 1
    # except Exception as e:
    #     err_count += 1
    #     raise e
    except snowflake.connector.errors.ProgrammingError as e:
        # default error message
        print(e)
        # customer error message
        err_count += 1
        failed_data.append(item)
        error_message = 'Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid)
        log(logfile_name, "ERROR", error_message)
        continue
        # conn.rollback()

conn.commit()
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

## export combined dict/object to json file in export folder
dict_objects = []
dict_objects.append(fluctuation_result)
dict_objects.append(latest_result)
merged_objects_string = merge_dict_string(dict_objects)

log_file_name = logfile_name+'_'+get_time_export_string()+'.log'
json_file_name = logfile_name+'_'+get_time_export_string()+'.json'
with open('./export/'+json_file_name, 'w') as outfile:
    outfile.write(merged_objects_string)

## export csv data for failed rows
error_file_name = 'error_'+logfile_name+'_'+get_time_export_string()+'.csv'
if len(failed_data) > 0:
    failed_data_df = pd.DataFrame(failed_data, columns=columns)
    failed_data_df.to_csv('./export/'+error_file_name)

## Load attachments if any before sending email
attachment_files = [str('/logs/'+log_file_name), str('/export/'+json_file_name), str('/export/'+error_file_name)]
send_email(logfile_name, 'GET/Currency_Fluctuation saved to DB', draft_email_body(str(count), str(err_count), str(time_taken_round)), attachment_files)
