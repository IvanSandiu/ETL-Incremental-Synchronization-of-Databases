# ETL-Incremental-Synchronization-of-Databases

## 📖 Overview  

This project implements an **incremental data synchronization pipeline** that transfers records from a **MySQL staging warehouse** to a **PostgreSQL data warehouse**.  

The pipeline:  
1. **Extracts** new records from the `sales_data` table in MySQL whose `rowid` is greater than the last `rowid` in the data warehouse.  
2. **Transforms** data minimally (ensures proper tuple or dict format for insertion).  
3. **Loads** the new records into the `sales_data` table in PostgreSQL using batch inserts.  
4. **Logs** the number of new records detected and inserted at each run.  

This approach ensures that only **incremental changes** are applied, avoiding re-inserting existing rows.

## Requirements

- **Python** 3.9+  
- Access to:
  - **MySQL** with the `sales_data` table
  - **PostgreSQL** with the `sales_data` table
- Packages:
  - `mysql-connector-python`
  - `psycopg2-binary`
  - (Optional, recommended) `python-dotenv` to manage environment variables

Install dependencies with:

```bash
pip install mysql-connector-python psycopg2-binary python-dotenv
```
## Configuration (Credentials and Connections)

Example *.env* file

```
# MySQL (staging)
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DB=sales
MYSQL_USER=root
MYSQL_PASSWORD=********

# PostgreSQL (DW)
PG_HOST=127.0.0.1
PG_PORT=5432
PG_DB=sales
PG_USER=postgres
PG_PASSWORD=********
```

## 🚀 How to Run

1. **(Optional)** Activate a Python virtual environment.  
2. Install dependencies:

```bash
pip install mysql-connector-python psycopg2-binary python-dotenv
```
