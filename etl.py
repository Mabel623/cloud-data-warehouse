import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging table from s3 to Redshift

    Parameters: 
    arg1 (function): create a cursor to allows Python code to execute PostgreSQL command in a database session
    arg2 (strings):redshift connection parameters, host(endpoint), databaseName, user name, user password and TCP port

    Returns: 
    Commit SQL COPY query (COPY files from S3 to Redshift)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into fact and dimension tables from staging tables

    Parameters: 
    arg1 (function): create a cursor to allows Python code to execute PostgreSQL command in a database session
    arg2 (strings):redshift connection parameters, host(endpoint), databaseName, user name, user password and TCP port

    Returns: 
    Commit SQL INSERT query (Extract, Transform, Load from staging tables to fact and dimension tables)
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Read database connection parameters from config file for connection, and insert data to fact and dimension tables followed with load staging tables from s3 to redshift
    
    Parameters: None

    Returns: 
    Commit SQL query (COPY files from S3 to Redshift, then Extract, Transform, Load from staging tables to fact and dimension tables)
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()