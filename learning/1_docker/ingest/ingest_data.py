import pandas as pd
import os
import argparse
from time import time
from sqlalchemy import create_engine
from loguru import logger

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    #pandas can open .gz
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'    

    #get file
    os.system(f"wget {url} -O {csv_name}")

    #create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #CSV file read iterator
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    #Total process
    t_begin = time()
    #Initialize the table in postgres
    df = next(df_iter)
    print(df.head(2))
    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name=table_name, con=engine, if_exists='replace')

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            logger.info('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            t_end = time()
            logger.info("Finished ingesting data into the postgres database, whole pipeline took %.3f second" % (t_end - t_begin))
            break    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=False, default="root", help='user name for postgres')
    parser.add_argument('--password', required=False, default="54321", help='password for postgres')
    parser.add_argument('--host', required=False, default="localhost", help='host for postgres')
    parser.add_argument('--port', required=False, default=5432, help='port for postgres')
    parser.add_argument('--db', required=False, default='ny_taxi', help='database name for postgres')
    parser.add_argument('--table_name', required=False, default='yellow_taxi_trips', help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    main(args)