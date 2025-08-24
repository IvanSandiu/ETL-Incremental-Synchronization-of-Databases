# Import libraries required for connecting to mysql /// pip3 install mysql-connector-python
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql /// python3 -m pip install psycopg2
import psycopg2
# Connect to MySQL
connMYSQL = mysql.connector.connect(user='root', password='kzDdsOj5ocsrl96nOGZJxzMh',host='172.21.218.7',database='sales')
# Connect to DB2 or PostgreSql
dsn_hostname = '172.21.175.187'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='HsTYU9R4bbHe2FrFBhcYMptQ'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="sales"           # i.e. "BLUDB"

try:
    conn = psycopg2.connect(
        database=dsn_database, 
        user=dsn_user,
        password=dsn_pwd,
        host=dsn_hostname, 
        port= dsn_port
    )
except Exception as e:
    print(f"Error connecting to PostgreSQL or fetching data: {e}")

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(rowid) FROM sales_data")
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    cursor = connMYSQL.cursor()
    query = "SELECT * FROM sales_data WHERE rowid > %s"
    cursor.execute(query, (rowid,))
    records = cursor.fetchall()
    cursor.close()
    return records

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO sales_data (rowid, product_id, customer_id, quantity)
        VALUES (%s, %s, %s, %s)
    """
    data = []
    for record in records:
        if isinstance(record, dict):
            data.append((
                record['rowid'],
                record['product_id'],
                record['customer_id'],
                record['quantity']
            ))
        elif isinstance(record, (list, tuple)):
            data.append(tuple(record))
        else:
            raise ValueError("Unsupported record format")
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connMYSQL.close()
# disconnect from DB2 or PostgreSql data warehouse 
conn.close()
# End of program
