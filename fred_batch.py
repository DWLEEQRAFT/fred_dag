from datetime import datetime
from functools import partialmethod

import pandas as pd
import fredapi as Fred
from fred_api  import *
import numpy as np
import math

import glob
from google.cloud import bigquery
from google.oauth2 import service_account

key_path = glob.glob("./config/*.json")[0]

credentials = service_account.Credentials.from_service_account_file(key_path)


client = bigquery.Client(credentials = credentials,
                         project = credentials.project_id)


tickers = []

fred = Fred()

for ticker in tickers :
    ticker_info = fred.get_series_info(ticker)
    # meta DB를 생성하고 테이블에서 latest_updated
    sql = f"""
    SELECT  realtime_start
    FROM    innate-plexus-345505.fred.fred_meta
    WHERE   id = @id
    
    """
    bigquery.ScalarQueryParameter('id', 'STRING', ticker)
    query_job = client.query(sql)
    fred_df = query_job.to_dataframe()

    db_latest_udpate = fred_df['realtimestart']
    if (ticker_info['latest_updated'] != db_latest_udpate) :
        continue
    latest_data = pd.DataFrame(fred.get_series_latest_release(ticker))

