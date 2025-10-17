from flask import Flask, request, render_template, redirect, url_for
import mysql.connector
import math
import os
import time
from mysql.connector import Error


app = Flask(__name__)

db_config = {
    'host': os.getenv('MYSQL_HOST', 'db'),
    'user': os.getenv('MYSQL_USER', 'user'),
    'password': os.getenv('MYSQL_PASSWORD', 'password'),
    'database': os.getenv('MYSQL_DATABASE', 'nyc311_db'),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}

@app.route('/index')
def index():
    return redirect(url_for('search'))


@app.route('/', methods=['GET', 'POST'])
def search():
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)

    page = int(request.args.get('page', 1))
    per_page = 20
    offset = (page - 1) * per_page

    start_date = request.args.get('start_date', '2023-12-01')
    end_date = request.args.get('end_date', '2023-12-31')
    borough = request.args.get('borough', '')
    complaint_type = request.args.get('complaint_type', '')

    query = """
        SELECT `unique_key`, `borough`, `complaint_type`, `created_date`
        FROM service_requests
        WHERE (`created_date` BETWEEN %s AND %s)
        AND (%s = '' OR `borough` = %s)
        AND (%s = '' OR `complaint_type` = %s)
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (start_date, end_date, borough, borough, complaint_type, complaint_type, per_page, offset))
    rows = cursor.fetchall()

    count_query = """
        SELECT COUNT(*) AS total
        FROM service_requests
        WHERE (`created_date` BETWEEN %s AND %s)
        AND (%s = '' OR `borough` = %s)
        AND (%s = '' OR `complaint_type` = %s)
    """
    cursor.execute(count_query, (start_date, end_date, borough, borough, complaint_type, complaint_type))
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    connection.close()

    return render_template(
        'search.html',
        rows=rows,
        page=page,
        total_pages=total_pages,
        start_date=start_date,
        end_date=end_date,
        borough=borough,
        complaint_type=complaint_type
    )

@app.route("/aggregate")
def aggregate():
    try:
        print("/aggregate route hit")

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        cursor.execute("SHOW TABLES;")
        tables = [t["Tables_in_" + db_config["database"]] for t in cursor.fetchall()]
        print("Available tables:", tables)

        if "service_requests" not in tables:
            return "<h2>Table 'service_requests' not found in database.</h2>", 500

        query = """
            SELECT borough, COUNT(*) AS total_complaints
            FROM service_requests
            WHERE borough IS NOT NULL AND borough <> ''
            GROUP BY borough
            ORDER BY total_complaints DESC;
        """
        cursor.execute(query)
        data = cursor.fetchall()

        print(f"Aggregate query returned {len(data)} rows")

        cursor.close()
        connection.close()

        if not data:
            return render_template("aggregate.html", data=[], message="No complaints found.")
        return render_template("aggregate.html", data=data, message=None)

    except mysql.connector.Error as e:
        print(f"MySQL error in /aggregate: {e}")
        return f"<h2>Database Error:</h2><pre>{e}</pre>", 500
    except Exception as e:
        print(f"Unexpected error in /aggregate: {e}")
        return f"<h2>Unexpected Error:</h2><pre>{e}</pre>", 500

def connect_with_retry(db_config, retries=10, delay=5):
    for i in range(retries):
        try:
            connection = connect_with_retry(db_config)
            if connection.is_connected():
                print("Connected to MySQL")
                return connection
        except Error as e:
            print(f"Attempt {i+1} failed: {e}")
            time.sleep(delay)
    raise Exception("Could not connect to database after several attempts")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
