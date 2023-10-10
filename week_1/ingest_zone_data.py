from time import time
import os
import argparse

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    # download csv file
    csv_name = 'zones.csv'
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name)

    df.head(0).to_sql(name=table, con=engine, if_exists='replace')

    df.to_sql(name=table, con=engine, if_exists='append')

    print('Inserted all zones')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest csv data into Postgres db')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='output table for postgres')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
