# This script is to interact with our database
# Helpful to run tests on this to ensure everything works as intended

# Load libraries
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_database_schema_and_row_counts(connection):
    """
    Retrieve schema information and row counts for tables in the public schema.

    Parameters:
    - connection: psycopg2 database connection.

    Returns:
    - A list of dictionaries containing schema information for each table.
    """
    cursor = connection.cursor()

    # Fetch tables in the database
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()

    schema_info = []
    for table in tables:
        table_name = table[0]
        # Fetch column details
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        columns = cursor.fetchall()

        # Fetch row count for the table
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
        """)
        row_count = cursor.fetchone()[0]

        schema_info.append({
            "table_name": table_name,
            "columns": columns,
            "row_count": row_count
        })

    return schema_info

def get_weather_forecast_by_airport(connection, airport_code):
    """
    Retrieve weather forecast data for a specific airport code from the weather_forecasts table.

    Parameters:
    - connection: psycopg2 database connection.
    - airport_code: Airport code for which weather forecast data is to be retrieved.

    Returns:
    - Weather forecast data for the specified airport code.
    """
    cursor = connection.cursor()

    cursor.execute("""
        SELECT *
        FROM weather_forecasts
        WHERE airport_code = %s
    """, (airport_code,))
    forecast_data = cursor.fetchall()

    return forecast_data

def print_all_rows_in_table(connection, table_name):
    """
    Print all rows in a given table.

    Parameters:
    - connection: psycopg2 database connection.
    - table_name: Name of the table from which rows are to be retrieved and printed.
    """
    cursor = connection.cursor()

    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    print(f"All rows in table {table_name}:")
    for row in rows:
        print(row)

# Can add query functions here
def print_rows_by_airport_code(connection, table_name, airport_code):
    """
    Print rows in a given table filtered by an airport_code.
    """
    cursor = connection.cursor()

    # Use a parameterized query for safety against SQL injection
    query = f"SELECT * FROM {table_name} WHERE airport_code = %s"
    cursor.execute(query, (airport_code,))

    rows = cursor.fetchall()

    if rows:
        print(f"Rows in table {table_name} for airport code '{airport_code}':")
        for row in rows:
            print(row)
    else:
        print(f"No rows found for airport code '{airport_code}' in table {table_name}.")


def main():
    try:
        connection = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME")
        )

        schema_info = get_database_schema_and_row_counts(connection)

        for table_info in schema_info:
            print(f"Table: {table_info['table_name']} - Rows: {table_info['row_count']}")
            for column in table_info["columns"]:
                print(f"    Column: {column[0]} - Type: {column[1]}")
            print("\n")

        # Test airport row function
        print_rows_by_airport_code(connection, 'airports', 'ACK')

        # Example usage of get_weather_forecast_by_airport function
        airport_code = "TEH"  # Replace with the desired airport code
        forecast_data = get_weather_forecast_by_airport(connection, airport_code)
        print("Weather forecast data for airport", airport_code)
        for row in forecast_data:
            print(row)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()
