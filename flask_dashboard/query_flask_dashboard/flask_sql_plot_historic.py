import os
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
import psycopg2
import plotly
import plotly.graph_objs as go
import json
from psycopg2 import sql
from datetime import datetime, timedelta

# Initialize Flask application
app = Flask(__name__)

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
        cursor.execute("SELECT DISTINCT airport_code FROM historic_weather ORDER BY airport_code;")
        return cursor.fetchall()


def get_weekly_weather_data(connection, airport_code, weather_variable):
    with connection.cursor() as cursor:
        # Fetch data and compute weekly average
        query = sql.SQL("""
            WITH weekly_data AS (
                SELECT date_trunc('week', date_recorded) AS week_start, AVG({field}) AS avg_value
                FROM historic_weather
                WHERE airport_code = %s
                GROUP BY week_start
                ORDER BY week_start ASC
            )
            SELECT week_start, avg_value FROM weekly_data;
        """).format(field=sql.Identifier(weather_variable))
        cursor.execute(query, (airport_code,))
        return cursor.fetchall()


@app.route('/graph', methods=['GET', 'POST'])
def historic_graph():
    connection = get_db_connection()
    airport_codes = get_airport_codes(connection)

    weather_variables = ['temperature_avg', 'dew_point_avg', 'humidity_avg', 'wind_speed_avg', 'pressure_avg']
    graph = None

    # Initialize airport_code with a default value, such as None or an empty string
    # Choose a value that makes sense for your application's logic
    airport_code = None

    if request.method == 'POST':
        airport_code = request.form.get('airport_code')
        weather_variable = request.form.get('weather_variable')

        if weather_variable not in weather_variables:
            return "Invalid weather variable selected", 400

        weather_data = get_weekly_weather_data(connection, airport_code, weather_variable)

        if weather_data:
            dates = [record[0] for record in weather_data]
            values = [record[1] for record in weather_data]

            # Prepare data points, accounting for missing weeks
            data_points = []
            for i, date in enumerate(dates[:-1]):
                next_date = dates[i + 1]
                gap = (next_date - date).days > 7

                if gap:
                    # If there's a gap, split into a new segment
                    data_points.append(go.Scatter(x=[date, dates[i]], y=[values[i], values[i]], mode='lines',
                                                  line=dict(color='grey', dash='dot')))
                    data_points.append(
                        go.Scatter(x=[next_date, next_date], y=[values[i + 1], values[i + 1]], mode='lines'))
                else:
                    # Continue current segment
                    if i == 0 or gap:  # Start new segment if first point or after a gap
                        data_points.append(go.Scatter(x=[date, next_date], y=[values[i], values[i + 1]], mode='lines'))
                    else:
                        data_points[-1].x += (date, next_date)
                        data_points[-1].y += (values[i], values[i + 1])

            # Generate the plot
            fig = go.Figure(data=data_points)
            fig.update_layout(
                title=f'Weekly Average {weather_variable.replace("_avg", "").capitalize()} Over Time in {airport_code}',
                xaxis_title='Date',
                yaxis_title=weather_variable.replace("_avg", "").capitalize(),
            )
            graph = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        else:
            print("No data found for the selected criteria.")

    return render_template('graph_historic.html', airport_codes=airport_codes, weather_variables=weather_variables, graph=graph)

if __name__ == '__main__':
    app.run(debug=True)
