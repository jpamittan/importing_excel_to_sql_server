import argparse
from humanfriendly import format_timespan
import json
import logging
import os
from pandas import read_excel
import pyodbc
import sys
import time


def load_config():
    """Load database configuration from config.json"""
    try:
        with open('config.json', "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("Error: config.json file not found")
        sys.exit(1)


def create_connection(config):
    try:
        # Create connection string using Windows Authentication
        conn = pyodbc.connect(
            'DRIVER={SQL Server};'
            f'SERVER={config["db"]["host"]},{config["db"]["port"]};'
            f'DATABASE={config["db"]["database"]};'
            'Trusted_Connection=yes;'
            f"UID={config["db"]["username"]};"
            f"PWD={config["db"]["password"]}",
        )

        return conn
    except FileNotFoundError:
        logging.error("Error: config.json not found.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        sys.exit(1)


def is_table_exist(cursor, table_name):
    check_table_exist_query = f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table_name}'"
    cursor.execute(check_table_exist_query)
    (table_exist,) = cursor.fetchone()

    return table_exist


def create_table(cursor, df, table_name):
    logging.info(f"Table {table_name} not found.")

    # Create table if it doesn't exist
    # Generate column definitions based on DataFrame dtypes
    create_columns = [
        # Auto add id column by default
        "[id] BIGINT IDENTITY(1,1) PRIMARY KEY",
        # Auto add created_at to monitor when it was inserted
        "[created_at] DATETIME NOT NULL DEFAULT(GETDATE())",
    ]
    for column, dtype in df.dtypes.items():
        sql_type = 'VARCHAR(255)'  # Default type
        if 'int' in str(dtype):
            sql_type = 'INT'
        elif 'float' in str(dtype):
            sql_type = 'FLOAT'
        elif 'datetime' in str(dtype):
            sql_type = 'DATETIME'
        create_columns.append(f"[{column}] {sql_type}")

    create_table_query = f"CREATE TABLE {table_name} ({', '.join(create_columns)})"

    logging.info(f"Executing table query: {create_table_query}")

    cursor.execute(create_table_query)


def row_import(
    conn,
    df,
    cursor,
    table_name,
    columns,
):
    logging.info("Importing by row...")
    for index, row in df.iterrows():
        placeholders = '?' * len(row)
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    conn.commit()

    # Verify the import
    total_rows = len(df)
    logging.info(f"Successfully imported {total_rows} rows into {table_name}")


def batch_import(
    df,
    cursor,
    table_name,
    columns,
    batch_size=1000
):
    logging.info("Importing by batch...")
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        for _, row in batch.iterrows():
            placeholders = '?' * len(row)
            insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
            cursor.execute(insert_query, tuple(row))
        cursor.commit()
        logging.info(f"Imported {min(i + batch_size, total_rows)} of {total_rows} rows")


def import_excel_to_sql(
    excel_file_path,
    table_name,
    is_batch=False
):
    start_time = time.time()

    try:
        # Load configuration
        logging.info("Loading configuration...")
        config = load_config()

        logging.info("Starting importing...")

        # Read Excel file
        df = read_excel(excel_file_path, index_col=None)

        # Clean column names (remove spaces and special characters)
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('[^A-Za-z0-9_]', '')

        # Connect to SQL Server
        conn = create_connection(config)
        cursor = conn.cursor()

        # Columns based on the excel file
        columns = []
        for column, dtype in df.dtypes.items():
            columns.append(f"[{column}]")

        if not is_table_exist(cursor, table_name):
            create_table(cursor, df, table_name)

        if is_batch:
            batch_import(df, cursor, table_name, columns)
        else:
            row_import(conn, df, cursor, table_name, columns)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(exc_type, fname, exc_tb.tb_lineno)
        logging.info("Rolling back...")
        conn.rollback()
        sys.exit(1)
    finally:
        # Close connections
        logging.info("Closing connection...")
        cursor.close()
        conn.close()

    end_time = time.time() - start_time
    logging.info(f"Finished. Duration {format_timespan(end_time)}")


if __name__ == "__main__":
    # Start
    parser = argparse.ArgumentParser(
        description='Import excel file to SQL Server.'
    )
    parser.add_argument(
        '-e',
        '--excel',
        required=True,
        help='Path location of excel file tobe imported to SQL Server.'
    )
    parser.add_argument(
        '-t',
        '--table',
        required=True,
        help='Target table name where to be inserted.'
    )
    parser.add_argument(
        '-b',
        '--batch',
        action="store_true",
        required=False,
        help='Enable batch mode upload.'
    )
    args = parser.parse_args()

    # Setup logs
    logging.basicConfig(
        filename="log.txt",
        filemode='a',
        format='%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(lineno)04d | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG
    )

    import_excel_to_sql(args.excel, args.table, args.batch)
