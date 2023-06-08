import csv
import psycopg2
from psycopg2 import Error

def create_tables(connection):
    try:
        cursor = connection.cursor()

        # Create Table for data_file1.csv
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_table1 (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                email VARCHAR(255),
                address VARCHAR(255)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_name ON data_table1 (name);")

        # Create Table for data_file2.csv
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_table2 (
                id SERIAL PRIMARY KEY,
                department_id INTEGER,
                employee_name VARCHAR(255),
                salary DECIMAL(10, 2)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_department_id ON data_table2 (department_id);")

        # Create Table for data_file3.csv
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_table3 (
                order_id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                price DECIMAL(10, 2),
                CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES customers (id),
                CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products (id)
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_customer_id ON data_table3 (customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_id ON data_table3 (product_id);")

        connection.commit()
        print("Tables created successfully!")

    except (Exception, Error) as error:
        connection.rollback()
        print("Error creating tables:", error)

def ingest_data(connection):
    try:
        cursor = connection.cursor()

        # Ingest data_file1.csv
        with open('data/data_file1.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                cursor.execute("INSERT INTO data_table1 (name, age, email, address) VALUES (%s, %s, %s, %s);", row)

        # Ingest data_file2.csv
        with open('data/data_file2.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                cursor.execute("INSERT INTO data_table2 (department_id, employee_name, salary) VALUES (%s, %s, %s);", row)

        # Ingest data_file3.csv
        with open('data/data_file3.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                cursor.execute("INSERT INTO data_table3 (customer_id, product_id, quantity, price) VALUES (%s, %s, %s, %s);", row)

        connection.commit()
        print("Data ingested successfully!")

    except (Exception, Error) as error:
        connection.rollback()
        print("Error ingesting data:", error)

def main():
    host = "localhost"
    database = "your_database"
    user = "your_user"
    password = "your_password"

    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

        create_tables(connection)
        ingest_data(connection)
