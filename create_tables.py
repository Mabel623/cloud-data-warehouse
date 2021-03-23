import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop existing tables in database before table creation 

    Parameters: 
    arg1 (function): create a cursor to allows Python code to execute PostgreSQL command in a database session
    arg2 (strings):redshift connection parameters, host(endpoint), databaseName, user name, user password and TCP port

    Returns: 
    Commit SQL query (existing tables are dropped)
    """
    
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging and fact tables in database

    Parameters: 
    arg1 (function): create a cursor to allows Python code to execute PostgreSQL command in a database session
    arg2 (strings):redshift connection parameters, host(endpoint), databaseName, user name, user password and TCP port

    Returns: 
    Commit SQL query (New staging, fact, dimension tables are created)
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Read database connection parameters from config file for connection, and create new tables followed with dropped existing tables
    
    Parameters: None

    Returns: 
    Commit SQL query (Drop and create tables)
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()