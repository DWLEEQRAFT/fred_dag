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

#key file directory
key_path = glob.glob("./config/*.json")[0]
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials,
                         project=credentials.project_id)

fred = Fred(api_key='51539bf992c74d576a870219fb703109')

# ticker 목록, method 존재하는 종목과, Kirin.Fred.api.py에 있는 티커 리스트가 달라, 유진님께 질의 예정
tickers = ['GDPPOT',
           'WILL5000PR',
           'DFEDTAR',
           'DFEDTARU',
           'DFF',
           'M1',
           'M2',
           'MZM',
           'DBAA',
           'GDPC1',
           'DTB3',
           'DGS2',
           'DGS5',
           'DGS10',
           'DGS30',
           'DTWEXB',
           'DTWEXBGS',
           'T10Y2Y',
           'PCE',
           'UNRATE',
           'CSUSHPISA',
           'CPIAUCSL',
           'M1V',
           'M2V',
           'MZMV',
           'CPILFESL',
           'DPCCRC1M027SBEA',
           'WPSFD4131',
           'WTISPLC',
           'CSUSHPISA',
           'INDPRO',
           'PCETRIM12M159SFRBDAL',
           'CP',
           'GFDEGDQ188S',
           'W006RC1Q027SBEA',
           'GPSAVE',
           'RMFSL',
           'WIMFSL'
           ]
# schema for table
schema = [bigquery.SchemaField("id", "STRING"),
          bigquery.SchemaField("realtime_end", "DATE"),
          bigquery.SchemaField("realtime_start", "DATE"),
          bigquery.SchemaField("title", "STRING"),
          bigquery.SchemaField("observation_end", "DATE"),
          bigquery.SchemaField("observation_start", "DATE"),
          bigquery.SchemaField("frequency", "STRING"),
          bigquery.SchemaField("frequency_short", "STRING"),
          bigquery.SchemaField("units", "STRING"),
          bigquery.SchemaField("units_short", "STRING"),
          bigquery.SchemaField("seasonal_adjustment", "STRING"),
          bigquery.SchemaField("seasonal_adjustment_short", "STRING"),
          bigquery.SchemaField("last_updated", "STRING"),
          bigquery.SchemaField("popularity", "STRING"),
          bigquery.SchemaField("notes", "STRING"),
          ]

job_config = bigquery.LoadJobConfig(schema=schema, autodetect=True, source_format=bigquery.enums.SourceFormat.CSV, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

table_id = f"innate-plexus-345505.fred.fred_meta"
file = f"./temp/temp.csv"


column_names = ["id", "realtime_end", "realtime_start", "title", "observation_end", "observation_start",
                "frequency", "frequency_short", "units", "units_short", "seasonal_adjustment",
                "seasonal_adjustment_short",
                "last_updated", "popularity", "notes"
                ]
tickers_info_df = pd.DataFrame(columns=column_names)


for ticker in tickers:
    ticker_info = fred.get_series_info(ticker)
    ticker_info_df = pd.DataFrame(ticker_info)
    ticker_info_df = ticker_info_df.transpose()

    temp_val = ticker_info_df.columns
    # 스키마 오류 때문에 csv로 변경해서 시도
    table_id = f"innate-plexus-345505.fred.fred_meta"
    file = f"./temp/temp.csv"
    ticker_info_df.to_csv(file)

    # csv에서 table 생성하여 업로드
    with open(file, "rb") as source_file:
        print(source_file)
        client.load_table_from_file(source_file, table_id, job_config=job_config)
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

        # client.load_table_from_dataframe("./temp/temp.csv", 'innate-plexus-345505.fred.fred_meta', job_config=job_config).result()

        print(ticker)

        sql = f"""
            SELECT  realtime_start
            FROM    innate-plexus-345505.fred.fred_meta
            WHERE   id = @id

            """
        bigquery.ScalarQueryParameter('id', 'STRING', ticker)
        query_job = client.query(sql)
        fred_df = query_job.to_dataframe()

        db_latest_udpate = fred_df['realtimestart']
        if (ticker_info['latest_updated'] != db_latest_udpate):
            continue
        latest_data = pd.DataFrame(fred.get_series_latest_release(ticker))







