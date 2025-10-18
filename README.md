# Assignment 3 – NYC 311 Service Requests App

In this assignment, I developed a complete end-to-end automation pipeline that takes real-world data and loads it into database. The goal is to design a system that automatically performs ETL (Extract, Transform, Load) operations on large datasets and display the results through Flask web application, which is also verified with automated Selenium tests.

The dataset I used was the NYC 311 Service Requests dataset, which contains information about complaints made by New York City residents. The ETL process reads the CSV file in batches of a few thousand rows at a time, cleans and transforms the data, and loads it into a MySQL database inside a Docker container. This batching method prevents memory overload and makes the script scalable for large datasets.

To ensure the application works correctly, I wrote Selenium automation tests. The positive test checks that valid search filters return results, while the negative test verifies that invalid filters display no results. Another test ensures the aggregate page loads properly. These tests run both locally and automatically through GitHub Actions in a CI/CD pipeline.

## Dataset slice and details
- **Dataset:** NYC 311 Service Requests (December 2023)  
- **Total rows loaded:** 265,000 (approx)  
- **Test subset:** 311_sample.csv
- **Source:** [NYC Open Data](https://data.cityofnewyork.us/)  

## ETL script
- ETL script reads CSV in chunks.  
- Column names are cleaned and standardized (lowercase, underscores).  
- Data inserted into `service_requests` table with use of SQLAlchemy.  
- Indexes created on `unique_key` and `created_date` for faster queries.  

## Steps to run the application
1. Build and start the containers:
    `docker compose up -d --build`
2. Load the ETL script:
    `docker exec -it prog8850-assignment3-app \ python /etl/etl.py --csv /app/data/311_2023_12.csv --chunksize 2000`
3. Verify if the data is loaded:
    `docker exec -it prog8850-assignment3-db-1 \ mysql -u root -proot -e "USE nyc311_db; SELECT COUNT(*) FROM service_requests;`
4. Open the browser at:
    `http://localhost:5000`
5. Run selenium tests:
    `pytest -q`