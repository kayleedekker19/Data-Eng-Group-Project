import os
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
import psycopg2
import plotly
import plotly.graph_objs as go
import json
from psycopg2 import sql

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
        cursor.execute("SELECT DISTINCT airport_code FROM weather_forecasts ORDER BY airport_code;")
        return cursor.fetchall()

def get_weather_data_for_graph(connection, airport_code, weather_variable):
    with connection.cursor() as cursor:
        # Use the psycopg2.sql module to safely create the dynamic query
        query = sql.SQL("""
            SELECT datetime_recorded, {field}
            FROM weather_forecasts
            WHERE airport_code = %s
            ORDER BY datetime_recorded ASC;
        """).format(field=sql.Identifier(weather_variable))
        cursor.execute(query, (airport_code,))
        return cursor.fetchall()


from psycopg2 import sql

@app.route('/graph', methods=['GET', 'POST'])
def show_forecast_plot():
    connection = get_db_connection()
    airport_codes = get_airport_codes(connection)
    # print("Airport Codes:", airport_codes)  # Debug print

    weather_variables = ['temperature', 'wind_speed', 'visibility', 'pressure', 'humidity']
    graph = None

    if request.method == 'POST':
        airport_code = request.form.get('airport_code')
        weather_variable = request.form.get('weather_variable')
        # print("Selected Airport Code:", airport_code)  # Debug print
        # print("Selected Weather Variable:", weather_variable)  # Debug print

        if weather_variable not in weather_variables:
            # print("Invalid weather variable selected")  # Debug print
            return "Invalid weather variable selected", 400

        weather_data = get_weather_data_for_graph(connection, airport_code, weather_variable)
        # print("Weather Data:", weather_data)  # Debug print

        if weather_data:
            dates = [record[0] for record in weather_data]
            values = [record[1] for record in weather_data]

            # print("Dates:", dates)  # Debug print
            # print("Values:", values)  # Debug print

            # Generate the plot
            fig = go.Figure(data=[go.Scatter(x=dates, y=values, mode='lines')])
            fig.update_layout(
                title=f'{weather_variable.capitalize()} Over Time at {airport_code}',
                xaxis_title='Datetime',
                yaxis_title=weather_variable.capitalize(),
            )
            graph = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        else:
            print("No data found for the selected criteria.")

    return render_template('graph.html', airport_codes=airport_codes, weather_variables=weather_variables, graph=graph)


if __name__ == '__main__':
    app.run(debug=True)

