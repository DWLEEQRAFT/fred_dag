import pandas as pd
from fredapi import Fred

import glob
from google.cloud import bigquery
from google.oauth2 import service_account

# key file directory
key_path = glob.glob("./config/*.json")[0]
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials,
                         project=credentials.project_id)

fred = Fred(api_key='51539bf992c74d576a870219fb703109')

# ticker updated - libor 금리 코드 미 존재, 질의 필요
tickers = ['CPILFESL',  # Price Level
           'DPCCRC1M027SBEA',
           'WPSFD4131', #  No [notes] data in fred series info
           'CPIAUCSL',
           'PCE',
           'WPSFD49207', #  No [notes] data in fred series info
           'PCETRIM12M159SFRBDAL',
           'WRMFSL',  # Money Flow
           'WIMFSL',
           'M1',
           'M2',
           'MZM',
           'M1V',
           'M2V',
           'MZMV',
           'XTEXVA01USM659S',  # Trade
           'XTIMVA01USM659S',
           'GGSAVE',  # Saving / Investment / Tax / Government
           'GPSAVE',
           'PSAVERT',
           'A822RO1Q156NBEA',
           'B006RO1Q156NBEA',
           'W006RC1Q027SBEA',
           'GFDEGDQ188S',
           'A191RO1Q156NBEA',
           'A001RO1Q156NBEA',  # Total Perspective
           'CP',
           'INDPRO',
           'CSUSHPISA',
           'TCU',
           'GDPC1',
           'UNRATE',  # Misc
           'VIXCLS', # VINTAGE DATES OUT OF MAXIMUN ERROR
           'WILL5000PR',
           'DFF',  # Maturity / Rate  # VINTAGE DATES OUT OF MAXIMUN ERROR
           'DGS1',
           'DGS2',
           'DGS3',
           'DGS5',
           'DGS7',
           'DGS10',
           'DGS20',
           'DGS30',
           'DAAA',
           'DBAA',
           #libor 금리, 조회되지 않음
             # 'USD1MTD156N',
             # 'USD3MTD156N',
             # 'USD6MTD156N',
             # 'USD12MTD156N',
           'TEDRATE',  # Spread
           'T3MFF',    # VINTAGE DATES OUT OF MAXIMUN ERROR
           'T6MFF',   # VINTAGE DATES OUT OF MAXIMUN ERROR
           'T1YFF',   # VINTAGE DATES OUT OF MAXIMUN ERROR
           'T5YFF',    # VINTAGE DATES OUT OF MAXIMUN ERROR
           'T10YFF',   # VINTAGE DATES OUT OF MAXIMUN ERROR
           'T10Y2Y',# VINTAGE DATES OUT OF MAXIMUN ERROR
           'T10Y3M',# VINTAGE DATES OUT OF MAXIMUN ERROR
           'AAAFF',
           'AAA10Y',
           'BAAFF',
           'BAA10Y',
           'BAMLH0A0HYM2'# VINTAGE DATES OUT OF MAXIMUN ERROR
           ]

# schema for table
schema_meta = [bigquery.SchemaField("id", "STRING"),
               bigquery.SchemaField("realtime_start", "DATE"),
               bigquery.SchemaField("realtime_end", "DATE"),
               bigquery.SchemaField("title", "STRING"),
               bigquery.SchemaField("observation_start", "DATE"),
               bigquery.SchemaField("observation_end", "DATE"),
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
schema_data = [bigquery.SchemaField("realtime_start", "DATE"),
               bigquery.SchemaField("date", "DATE"),
               bigquery.SchemaField("value", "STRING"),
               bigquery.SchemaField("last_updated", "STRING"),
               bigquery.SchemaField("id", "STRING")
               ]

table_id_meta = f"innate-plexus-345505.fred.fred_meta"
table_id_data = f"innate-plexus-345505.fred.fred_data"
file_meta = f"./temp/temp.csv"
file_data = f"./temp/temp_data.csv"
column_names = ["id", "realtime_end", "realtime_start", "title", "observation_end", "observation_start", "frequency",
                "frequency_short", "units", "units_short", "seasonal_adjustment", "seasonal_adjustment_short",
                "last_updated", "popularity", "notes"
                ]



for ticker in tickers:
    # Get Fred_series_info data by FRED API
    ticker_info = fred.get_series_info(ticker)
    ticker_info_df_temp = pd.DataFrame(ticker_info)

    ticker_info_df = pd.DataFrame(columns=column_names)
    ticker_info_df = pd.concat([ticker_info_df, pd.DataFrame(ticker_info_df_temp.transpose())])

    # GET Fred_series_info in DB
    sql = f"""
            SELECT  last_updated
            FROM    innate-plexus-345505.fred.fred_meta
            WHERE   id = @id

            """
    job_config_load = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", ticker)
        ]
    )
    query_job = client.query(sql, job_config=job_config_load)
    fred_df = query_job.to_dataframe()
    db_latest_updated = fred_df['last_updated'].values

    print(ticker + " from fred  : " + ticker_info_df['last_updated'].values)
    print(ticker + " from db    : " + db_latest_updated)




    # DB에서 조회한 last_update 가 없거나, ticker 자체가 없는 경우
    if ticker_info_df['last_updated'].iloc[0] != db_latest_updated or len(db_latest_updated) == 0:
        # meta 데이터 갱신
        if len(db_latest_updated) == 0:
            print(ticker + " meta uploaded ")
            # meta 없음
            job_config_meta = bigquery.LoadJobConfig(schema=schema_meta,
                                                     autodetect=True,
                                                     source_format=bigquery.enums.SourceFormat.CSV,
                                                     write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            ticker_info_df.to_csv(file_meta, index=False)
            # csv에서 table 생성하여 업로드
            with open(file_meta, "rb") as source_file:
                #print(source_file)
                client.load_table_from_file(source_file, table_id_meta, job_config=job_config_meta).result()
                table = client.get_table(table_id_meta)
                #
        else:
            # meta는 존재하나, update 필요
            sql_update = f"""
                UPDATE  fred.fred_meta
                SET     last_updated = @last_updated
                WHERE   id = @id
        
                """
            job_config_update_meta = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("last_updated", "STRING", ticker_info_df['last_updated'].iloc[0]),
                    bigquery.ScalarQueryParameter("id", "STRING", ticker),
                ]
            )
            # meta update
            query_job = client.query(sql_update, job_config=job_config_update_meta)
            print("ticker [" + ticker + "] meta updated")

        # tickers_info_df = pd.concat([tickers_info_df, ticker_info_df])

    # get series_data
    row_updated = pd.DataFrame()

    if len(db_latest_updated) == 0:
        # 데이터가 존재하지 않을 경우, series를 모두 업로드
        print("All release uploaded : " + ticker)
        row_updated = pd.DataFrame(fred.get_series_all_releases(ticker))
    else:
        print("latest series uploaded : " + ticker)
        update_series = pd.DataFrame(fred.get_series_all_releases(ticker))
        update_series['realtime_start'] = ticker_info_df['realtime_start'].iloc[0]
        for row in update_series:
            if row['realtime_start'] > db_latest_updated.iloc[0]:
                row_updated = pd.concat([row_updated, row])

        #print(update_series)

    row_updated['last_updated'] = ticker_info_df['last_updated'].iloc[0]
    row_updated['id'] = ticker
    row_updated.rename(columns={"id": "id_new"})
    row_updated.to_csv(file_data, index=False)

    job_config_upload_series = bigquery.LoadJobConfig(schema=schema_data,
                                                      autodetect=True,
                                                      source_format=bigquery.enums.SourceFormat.CSV,
                                                      write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    with open(file_data, "rb") as source_file:
        client.load_table_from_file(source_file, table_id_data, job_config=job_config_upload_series).result()
        table = client.get_table(table_id_data)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id_data
            )
        )
