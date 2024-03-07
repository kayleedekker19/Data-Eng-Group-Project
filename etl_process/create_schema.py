import psycopg2


def recreate_database_schema(connection):
    cursor = connection.cursor()

    # Drop existing tables
    drop_tables_queries = [
        "DROP TABLE IF EXISTS weather_forecasts, historic_weather, airports CASCADE;"
    ]
    for query in drop_tables_queries:
        cursor.execute(query)

    # Create new tables
    create_tables_queries = [
        """
        CREATE TABLE airports (
            airport_code VARCHAR(10) PRIMARY KEY,
            airport_name VARCHAR(100),
            city_name VARCHAR(100),
            country_name VARCHAR(100),
            country_code VARCHAR(10),
            latitude FLOAT,
            longitude FLOAT,
            world_area_code INT,
            city_name_geo_name_id INT,
            country_name_geo_name_id INT
        );
        """,
        """
        CREATE TABLE historic_weather (
            record_id SERIAL PRIMARY KEY,
            airport_code VARCHAR(10) REFERENCES airports(airport_code),
            date_recorded DATE,
            temperature_max FLOAT,
            temperature_avg FLOAT,
            temperature_min FLOAT,
            dew_point_max FLOAT,
            dew_point_avg FLOAT,
            dew_point_min FLOAT,
            humidity_max INT,
            humidity_avg INT,
            humidity_min INT,
            wind_speed_max FLOAT,
            wind_speed_avg FLOAT,
            wind_speed_min FLOAT,
            pressure_max FLOAT,
            pressure_avg FLOAT,
            pressure_min FLOAT,
            precipitation FLOAT
        );
        """,
        """
        CREATE TABLE weather_forecasts (
            forecast_id SERIAL PRIMARY KEY,
            airport_code VARCHAR(10) REFERENCES airports(airport_code),
            datetime_recorded TIMESTAMP,
            temperature FLOAT,
            feels_like FLOAT,
            temperature_min FLOAT,
            temperature_max FLOAT,
            pressure INT,
            sea_level INT,
            ground_level INT,
            humidity INT,
            weather_main VARCHAR,
            weather_description VARCHAR,
            clouds_all INT,
            wind_speed FLOAT,
            wind_deg INT,
            wind_gust FLOAT,
            visibility INT,
            pop FLOAT,
            rain_3hr FLOAT,
            pod CHAR(1),
            country CHAR(2),
            timezone INT,
            latitude DECIMAL,
            longitude DECIMAL,
            sunrise BIGINT,
            sunset BIGINT
        );
        """
    ]
    for query in create_tables_queries:
        cursor.execute(query)

    # Commit changes
    connection.commit()
    print("Database schema recreated successfully.")


try:
    connection = psycopg2.connect(
        user=<"username">,
        password=<"password">,
        host=<"host_ip">,
        port=<"port_number">,
        database=<"database_name">
    )

    # Recreate the database schema
    recreate_database_schema(connection)

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
