
import os
from dotenv import load_dotenv
import psycopg2
from flask import Flask, render_template
from markupsafe import Markup
import pandas as pd
import plotly.express as px

# # Initialize Flask application
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

def fetch_airports_data():
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT airport_code, airport_name, latitude, longitude, country_name
            FROM airports;
        """)
        data = cursor.fetchall()
    connection.close()
    return data

def create_figure():
    data = fetch_airports_data()
    # Convert data to Pandas DataFrame
    columns = ['airport_code', 'airport_name', 'latitude', 'longitude', 'country_name']
    df = pd.DataFrame(data, columns=columns)

    # List of airport codes to exclude - replace these placeholders with actual codes
    excluded_airport_codes = ['ZWI', 'TEH', 'OIA', 'TCS', 'SZL', 'PUO', 'YUB', 'YTK']

    # Filter out the rows with the excluded airport codes
    df = df[~df['airport_code'].isin(excluded_airport_codes)]

    mapbox_access_token = 'pk.eyJ1Ijoia2FydGFncmFtIiwiYSI6ImNsdGl3ZjhhdTBqZDQyaXBoeHdqZmZvY2MifQ.lAX-1WPYrpKX97HBk4ppAw'

    fig = px.scatter_mapbox(
        df,
        lat='latitude',
        lon='longitude',
        hover_name='airport_code',
        color='country_name',
        zoom=1,
        height=900,
        width=1200
    )

    fig.update_layout(
        mapbox_style="light",
        mapbox_accesstoken=mapbox_access_token,
        uirevision='constant',
        margin=dict(r=20, t=20, l=20, b=20),
        showlegend=False
    )

    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br><br>" +
                      "Airport Name: %{customdata[0]}<br>" +
                      "Latitude: %{lat}<br>" +
                      "Longitude: %{lon}<br>",
        customdata=df[['airport_name']]
    )

    return fig.to_html(full_html=False)


# # Initialize Flask application
# app = Flask(__name__)

# @app.route('/')
def index():
    fig_html = create_figure()
    return render_template('index.html', fig_html=Markup(fig_html))

# if __name__ == '__main__':
#     app.run(debug=True)
