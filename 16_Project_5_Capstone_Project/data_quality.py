from sql_queries import tables
import pandas as pd
import configparser
import psycopg2


def records(cur, conn, tables):
    
    """
    Checker for the records inserted in each table
    
    Args:
        cur (:obj:`psycopg2.extensions.cursor`) ► Cursor for connection
        conn (:obj:`psycopg2.extensions.connection`) ► database connection
        tables ► list of the tables in the DB
    """
    
    for table in tables:
        
        print(f"\n\n-------Checking number of records from {table} table---------")
        
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        records = cur.fetchall()
        conn.commit()
            
        
        if int(records[0][0]) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
            
        num_records = records[0][0]
            
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
        print(f"Data quality on table {table} check passed with {records[0][0]} records")
    
    
def display(cur, conn, tables):
    
    """
    Checker for the first 5 rows in each table
    
    Args:
        cur (:obj:`psycopg2.extensions.cursor`) ► Cursor for connection
        conn (:obj:`psycopg2.extensions.connection`) ► database connection
        tables ► list of the tables in the DB
    """
    
    for table in tables:

        print(f"\n\n-------Checking first 5 rows from {table} table---------")
        cur.execute(f"SELECT * FROM {table} LIMIT 5")
        print("\n",pd.DataFrame(cur.fetchall()))
        conn.commit()
    
def main():
    """
    Main function to connect to the cluster, reset the DB, 
    and call functions to create the tables 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('\nCluster connection succesfull')

    records(cur, conn, tables)
    display(cur, conn, tables)
    
    print('\n\n\t\tData quality checker executed\n\n')

    conn.close()


if __name__ == "__main__":
    main()