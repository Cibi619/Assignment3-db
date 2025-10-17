import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
import time

DATE_COLS = ["Created Date", "Closed Date"]
KEEP_COLS = [
    "Unique Key", "Created Date", "Closed Date", "Agency", "Agency Name",
    "Complaint Type", "Descriptor", "Incident Zip", "Incident Address",
    "City", "Borough", "Status", "Resolution Description",
    "Latitude", "Longitude"
]

BOROUGH_FIX = {
    "": None, "Unspecified": None, "N/A": None, None: None,
    "BRONX": "BRONX", "BROOKLYN": "BROOKLYN", "MANHATTAN": "MANHATTAN",
    "QUEENS": "QUEENS", "STATEN ISLAND": "STATEN ISLAND"
}

def get_engine():
    user = os.getenv("MYSQL_USER", "user")
    pwd = os.getenv("MYSQL_PASSWORD", "password")
    host = os.getenv("MYSQL_HOST", "db")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    db   = os.getenv("MYSQL_DATABASE", "nyc311_db")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True)

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    existing = [c for c in KEEP_COLS if c in df.columns]
    df = df[existing].copy()

    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    if "Borough" in df.columns:
        df["Borough"] = df["Borough"].map(lambda b: BOROUGH_FIX.get(b, str(b).upper() if isinstance(b, str) else None))

    df.rename(columns={
        "Unique Key": "unique_key",
        "Created Date": "created_date",
        "Closed Date": "closed_date",
        "Agency": "agency",
        "Agency Name": "agency_name",
        "Complaint Type": "complaint_type",
        "Descriptor": "descriptor",
        "Incident Zip": "incident_zip",
        "Incident Address": "incident_address",
        "City": "city",
        "Borough": "borough",
        "Status": "status",
        "Resolution Description": "resolution_description",
        "Latitude": "latitude",
        "Longitude": "longitude"
    }, inplace=True)

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=str, default='data/311_2023_12.csv', help='Path to CSV file')
    parser.add_argument('--chunksize', type=int, default=2000, help='Batch size for inserts')
    args = parser.parse_args()

    csv_path = args.csv
    chunksize = args.chunksize
    print(f"Loading CSV from: {csv_path}")

    engine = create_engine("mysql+pymysql://root:root@localhost:3306/nyc311_db")
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
        chunk.to_sql(name="service_requests", con=engine, if_exists="append", index=False)

if __name__ == "__main__":
    main()
