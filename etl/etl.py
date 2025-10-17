import pandas as pd
import os
import time
import argparse
from sqlalchemy import create_engine

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=str, default='/app/data/311_2023_12.csv')
    parser.add_argument('--chunksize', type=int, default=2000, help='Batch size for inserts')
    args = parser.parse_args()

    csv_path = args.csv
    chunksize = args.chunksize

    print(f"Loading CSV from: {csv_path}")
    start_time = time.time()

    user = os.getenv('MYSQL_USER', 'root')
    password = os.getenv('MYSQL_PASSWORD', 'root')
    host = os.getenv('MYSQL_HOST', 'db')
    database = os.getenv('MYSQL_DATABASE', 'nyc311_db')

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:3306/{database}")

    total_rows = 0
    first_chunk = True

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
        chunk.columns = (
            chunk.columns
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('/', '_')
        )

        try:
            if first_chunk:
                chunk.to_sql(name='service_requests', con=engine, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk.to_sql(name='service_requests', con=engine, if_exists='append', index=False)

            total_rows += len(chunk)
            print(f"Inserted {len(chunk)} rows... Total: {total_rows}")

        except Exception as e:
            print(f"Error inserting chunk: {e}")
            continue

    print(f"\nETL Completed. Total rows inserted: {total_rows}")
    print(f"Duration: {round(time.time() - start_time, 2)} seconds")

if __name__ == "__main__":
    main()
