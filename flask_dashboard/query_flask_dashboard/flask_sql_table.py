
import os
from dotenv import load_dotenv
from flask import Flask, render_template, request
import psycopg2

# Initialize Flask application
# app = Flask(__name__)

# Load environment variables
load_dotenv()

# Database connection function
def get_db_connection():
    connection = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME")
    )
    return connection

def get_airport_codes(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT airport_code FROM weather_forecasts ORDER BY airport_code;")
        return cursor.fetchall()

def get_unique_dates(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT DATE(datetime_recorded) FROM weather_forecasts ORDER BY DATE(datetime_recorded);")
        return cursor.fetchall()

def get_weather_forecast(connection, airport_code, date, time):
    datetime_recorded = f"{date} {time}"
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT airport_code, datetime_recorded, temperature, wind_speed, wind_deg, wind_gust, visibility, pop, rain_3hr, weather_description
            FROM weather_forecasts
            WHERE airport_code = %s AND DATE(datetime_recorded) = DATE(%s) AND EXTRACT(HOUR FROM datetime_recorded) = EXTRACT(HOUR FROM TIMESTAMP %s)
            ORDER BY datetime_recorded;
        """, (airport_code, datetime_recorded, datetime_recorded))
        return cursor.fetchall()

# @app.route('/', methods=['GET', 'POST'])
def show_query_table():
    connection = get_db_connection()
    airport_codes = get_airport_codes(connection)
    unique_dates = get_unique_dates(connection)  # Retrieve unique dates
    time_options = ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00']
    weather_data = []
    if request.method == 'POST':
        airport_code = request.form.get('airport_code')
        date = request.form.get('date')
        time = request.form.get('time')
        weather_data = get_weather_forecast(connection, airport_code, date, time)
    return render_template('query_table.html', airport_codes=airport_codes, unique_dates=unique_dates, weather_data=weather_data, time_options=time_options)

# connection = get_db_connection()
# get_weather_forecast(connection, 'ERM', date, time)

# def dashboard():
#     connection = get_db_connection()
#     airport_codes = get_airport_codes(connection)
#     unique_dates = get_unique_dates(connection)  # Retrieve unique dates
#     time_options = ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00']
#     weather_data = []
#     if request.method == 'POST':
#         airport_code = request.form.get('airport_code')
#         date = request.form.get('date')
#         time = request.form.get('time')
#         weather_data = get_weather_forecast(connection, airport_code, date, time)
#     return render_template('query_table.html', airport_codes=airport_codes, unique_dates=unique_dates, weather_data=weather_data, time_options=time_options)


# if __name__ == '__main__':
#     app.run(debug=True)
