import mysql.connector


# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
    connection = mysql.connector.connect(
        host=host_name,
        username=user_name,
        password=user_password
    )
    print(f"Database Server Connection Created Successfully...")
    return connection


# This method will create the database and make it an active database
def create_and_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    cursor = connection.cursor()
    try:
        cursor.execute("DROP DATABASE IF EXISTS " + db_name)
        cursor.execute("CREATE DATABASE " + db_name)
        cursor.execute("USE " + switch_db)
        print(f"Using DB {switch_db}")
    except Exception as err:
        print(f"Error in creating and switching database: '{err}'")


# This method will establish the connection with the newly created DB
def create_db_connection(host_name, user_name, user_password, db_name):
    connector = mysql.connector.connect(
        host=host_name,
        username=user_name,
        password=user_password,
        db=db_name
    )
    print(f"Connected To Database {db_name}")
    return connector


# Use this function to create the tables in a database
def create_table(connection, table_creation_statement):
    cursor = connection.cursor()
    cursor.execute(table_creation_statement)
    connection.commit()
    print("Table Is Created")


# Perform all single insert statments in the specific table through a single function call
def create_insert_query(connection, query):
    # This method will perform creation of the table
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Insert operation successful")
    except Exception as err:
        print(f"Error in insert query: '{err}'")


# retrieving the data from the table based on the given query
def select_query(connection, query):
    # fetching the data points from the table 
    # This method will perform creation of the table
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result


# Execute multiple insert statements in a table
def insert_many_records(connection, sql, val):
    cursor = connection.cursor()
    cursor.executemany(sql, val)
    connection.commit()
    print("Many Records Insertion Is Successful")


