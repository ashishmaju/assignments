import csv
import database as db

PW = "root"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"
DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "localhost"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = '../config/'

USER = 'users'
PRODUCTS = 'products'
ORDER = 'orders'

connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)

# Create the tables through python code here
# if you have created the table in UI, then no need to define the table structure
# If you are using python to create the tables, call the relevant query to complete the creation

# Creating users table
create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar(10) PRIMARY KEY,
        user_name varchar(45) NOT NULL,
        user_email varchar(45) NOT NULL,
        user_password varchar(45) NOT NULL,
        user_address varchar(45) NULL,
        is_vendor tinyint(1) DEFAULT 0
    )
    """

# create Products table
create_products_table = """
    CREATE TABLE IF NOT EXISTS  products (
      product_id varchar(45) NOT NULL PRIMARY KEY,
      product_name varchar(45) NOT NULL,
      product_description varchar(100) NOT NULL,
      product_price float(45) NOT NULL,
      emi_available varchar(10) NOT NULL,
      vendor_id varchar(10) NOT NULL,
      CONSTRAINT `fk_vendor_id` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`)
    )
    """
# create orders table
create_orders_table = """
    CREATE TABLE IF NOT EXISTS  orders (
      order_id int NOT NULL PRIMARY KEY,
      total_value float(45) NOT NULL,
      order_quantity int NOT NULL,
      reward_point int NOT NULL,
      vendor_id varchar(10) NOT NULL,
      customer_id varchar(10) NOT NULL,
      CONSTRAINT `vendor_id` FOREIGN KEY (`vendor_id`) REFERENCES `users` (`user_id`),
      CONSTRAINT `customer_id` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    )
    """
# creating customer leaderboard table
create_customer_leaderboard = """
    CREATE TABLE customer_leaderboard (
      customer_id varchar(10) NOT NULL PRIMARY KEY,
      total_value float(45) NOT NULL,
      customer_name varchar(50) NOT NULL,
      customer_email varchar(50) NOT NULL,
      CONSTRAINT `fk_customer_id` FOREIGN KEY (`customer_id`) REFERENCES `users` (`user_id`)
    )
    """

connection = db.create_server_connection(LOCALHOST, ROOT, PW)
# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)
print("Initiating creation of tables: ")

db.create_table(connection, create_users_table)  # Execute our defined query
print("Users table created")
db.create_table(connection, create_products_table)  # Execute our defined query
print("Products table created")
db.create_table(connection, create_orders_table)  # Execute our defined query
print("Orders table created")
db.create_table(connection, create_customer_leaderboard)  # Execute our defined query
print("Customer leaderboard table created")
print("Adding data in User table.")

with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    query = '''
    INSERT INTO users (user_id, user_name, user_email, user_password, user_address, is_vendor) 
    VALUES (%s, %s, %s, %s, %s, %s)
    '''
    db.insert_many_records(connection, query, val)
    print("Data is added into Users table")

print("Adding data in Products table.")
with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    query = '''
        INSERT INTO products (product_id, product_name, product_price, product_description, vendor_id, emi_available) 
    VALUES (%s, %s, %s, %s, %s, %s)
    '''
    db.insert_many_records(connection, query, val)
    print("Data is added into Products table")

print("Adding data in Order table.")
with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    val.pop(0)
    """
    Here we have accessed the file data and saved into the val data struture, which list of tuples. 
    Now you should call appropriate method to perform the insert operation in the database. 
    """
    query = '''
        INSERT INTO orders (order_id, customer_id, vendor_id, total_value, order_quantity, reward_point) 
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
    db.insert_many_records(connection, query, val)
    print("Data is added into Orders table")
