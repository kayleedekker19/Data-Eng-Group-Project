# import psycopg2
#
# try:
#     connection = psycopg2.connect(
#         user="postgres",
#         password="dataeng",
#         host="34.89.0.15",
#         port="5432",
#         database="Weather_database"
#     )
#     cursor = connection.cursor()
#
#     # Example query
#     cursor.execute("SELECT version();")
#     record = cursor.fetchone()
#     print("Connected to PostgreSQL server:", record)
#
# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to PostgreSQL:", error)
# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("PostgreSQL connection is closed")

# import psycopg2
#
# def get_database_schema(connection):
#     cursor = connection.cursor()
#
#     # Fetch tables in the database
#     cursor.execute("""
#         SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema = 'public'
#     """)
#     tables = cursor.fetchall()
#
#     schema_info = []
#     for table in tables:
#         table_name = table[0]
#         cursor.execute("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = %s
#         """, (table_name,))
#         columns = cursor.fetchall()
#         schema_info.append({
#             "table_name": table_name,
#             "columns": columns
#         })
#
#     return schema_info
#
# try:
#     connection = psycopg2.connect(
#         user="postgres",
#         password="dataeng",
#         host="34.89.0.15",
#         port="5432",
#         database="Weather_database"
#     )
#
#     schema_info = get_database_schema(connection)
#
#     for table_info in schema_info:
#         print("Table:", table_info["table_name"])
#         for column in table_info["columns"]:
#             print("    Column:", column[0], "- Type:", column[1])
#         print("\n")
#
# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to PostgreSQL:", error)
# finally:
#     if connection:
#         connection.close()
#         print("PostgreSQL connection is closed")


#
# import psycopg2
#
# # Function to get the schema of the database
# def get_database_schema(connection):
#     cursor = connection.cursor()
#
#     # Fetch tables in the database
#     cursor.execute("""
#         SELECT table_name
#         FROM information_schema.tables
#         WHERE table_schema = 'public'
#     """)
#     tables = cursor.fetchall()
#
#     schema_info = []
#     for table in tables:
#         table_name = table[0]
#         cursor.execute("""
#             SELECT column_name, data_type
#             FROM information_schema.columns
#             WHERE table_name = %s
#         """, (table_name,))
#         columns = cursor.fetchall()
#         schema_info.append({
#             "table_name": table_name,
#             "columns": columns
#         })
#
#     return schema_info
#
# try:
#     # Connect to the PostgreSQL database
#     connection = psycopg2.connect(
#         user="postgres",
#         password="dataeng",
#         host="34.89.0.15",
#         port="5432",
#         database="Weather_database"
#     )
#
#     # Get the schema information of the database
#     schema_info = get_database_schema(connection)
#
#     # Print the schema information
#     for table_info in schema_info:
#         print("Table:", table_info["table_name"])
#         for column in table_info["columns"]:
#             print("    Column:", column[0], "- Type:", column[1])
#         print("\n")
#
# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to PostgreSQL:", error)
# finally:
#     # Close the database connection
#     if connection:
#         connection.close()
#         print("PostgreSQL connection is closed")
#
#
# import psycopg2
#
# def execute_query(connection, query):
#     cursor = connection.cursor()
#     cursor.execute(query)
#     rows = cursor.fetchall()
#     cursor.close()
#     return rows
#
# try:
#     connection = psycopg2.connect(
#         user="postgres",
#         password="dataeng",
#         host="34.89.0.15",
#         port="5432",
#         database="Weather_database"
#     )
#
#     # Example queries
#
#     # Query 1: Select all records from the airports table
#     query_1 = "SELECT * FROM airports;"
#     result_1 = execute_query(connection, query_1)
#     print("Query 1 Result:")
#     for row in result_1:
#         print(row)
#
#     # Query 2: Select airport code, latitude, and longitude from airports for a specific country
#     country_name = 'Your_Country_Name'
#     query_2 = f"SELECT airport_code, latitude, longitude FROM airports WHERE country_name = '{country_name}';"
#     result_2 = execute_query(connection, query_2)
#     print("\nQuery 2 Result:")
#     for row in result_2:
#         print(row)
#
#     # Query 3: Select max temperature recorded from historic_weather for each airport
#     query_3 = "SELECT airport_code, MAX(temperature_max) AS max_temperature FROM historic_weather GROUP BY airport_code;"
#     result_3 = execute_query(connection, query_3)
#     print("\nQuery 3 Result:")
#     for row in result_3:
#         print(row)
#
#
# except (Exception, psycopg2.Error) as error:
#     print("Error while connecting to PostgreSQL:", error)
# finally:
#     if connection:
#         connection.close()
#         print("PostgreSQL connection is closed")


import psycopg2


def get_database_schema_and_row_counts(connection):
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


try:
    connection = psycopg2.connect(
        user="postgres",
        password="dataeng",
        host="34.89.0.15",
        port="5432",
        database="Weather_database"
    )

    schema_info = get_database_schema_and_row_counts(connection)

    for table_info in schema_info:
        print(f"Table: {table_info['table_name']} - Rows: {table_info['row_count']}")
        for column in table_info["columns"]:
            print(f"    Column: {column[0]} - Type: {column[1]}")
        print("\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)
finally:
    if connection:
        connection.close()
        print("PostgreSQL connection is closed")
