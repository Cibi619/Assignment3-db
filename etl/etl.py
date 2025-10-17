import os
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
    engine = get_engine()
    csv_path = "/data/311_2023_12.csv"
    chunksize = 50000

    total_inserted = 0
    start_time = time.time()

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
        df = clean_chunk(chunk)
        if not df.empty:
            df.to_sql("service_requests", engine, if_exists="append", index=False, method="multi", chunksize=10000)
            total_inserted += len(df)
            print(f"Inserted {len(df)} rows (total {total_inserted})")

    elapsed = time.time() - start_time
    print(f"\nETL complete: Inserted {total_inserted} rows in {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
