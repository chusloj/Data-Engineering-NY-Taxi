import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_output_name = 'output.parquet'

    os.system(f"curl -o {parquet_output_name} {url}")

    trips = pq.read_table(f"{parquet_output_name}")
    trips = trips.to_pandas()
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # engine.connect()
     
    trips.tpep_pickup_datetime = pd.to_datetime(trips.tpep_pickup_datetime)
    trips.tpep_dropoff_datetime = pd.to_datetime(trips.tpep_dropoff_datetime)
    
    # Ingest data
    trips.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') # insert schema
    
    trips_subset = trips.head(n = 300000)
    trips_subset.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == "__main__":

    # CLI
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres.')

    # user, password, host/port, DB name, table name, URL of parquet file
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table to be created and populated')
    parser.add_argument('--url', help='URL of parquet file to be downloaded')

    args = parser.parse_args()

    main(args)
