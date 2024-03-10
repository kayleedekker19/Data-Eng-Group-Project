from flask import Flask, request
# Import the modularized route functions and rename them to avoid name conflicts
from flask_sql_table import show_query_table as sql_table_show
from flask_sql_plot_forecast import show_forecast_plot as forecast_plot_show
from flask_sql_plot_historic import historic_graph as historic_plot_show
from flask_sql_plot_airport import index as airport_index_show
from flask_google_earth import show_map as google_earth_show


app = Flask(__name__)

#Insert visualizations here
# Google Earth Map
@app.route('/', methods=['GET', 'POST'])
def google_earth_map():
    # Call the show_map function from flask_google_earth module
    return google_earth_show()

# Query Table
@app.route('/query-table', methods=['GET', 'POST'])
def query_table_route():
    # Call the show_query_table function from flask_sql_table module
    return sql_table_show()

# Forecast Plot
@app.route('/forecast-plot', methods=['GET', 'POST'])
def forecast_plot_route():
    # Call the show_forecast_plot function from flask_sql_plot_forecast module
    return forecast_plot_show()

# Historic Plot
@app.route('/historic-plot', methods=['GET', 'POST'])
def historic_plot_route():
    # Call the historic_graph function from flask_sql_plot_historic module
    return historic_plot_show()

# Airport Plot
@app.route('/airport-plot')
def airport_index_route():
    # Call the index function from flask_sql_plot_airport module
    return airport_index_show()

if __name__ == '__main__':
    app.run(debug=True)

