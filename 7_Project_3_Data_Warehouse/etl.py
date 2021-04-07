import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to insert data into staging tables
    """
    
    print('\n\t\tInserting data into staging tables ...')
    
    for query in copy_table_queries:
        print('\nInserting data into table ► ' + query.split()[1])
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function to insert data into the tables
    """
    
    print('\n\n\t\tInserting data into schema tables ...')
    
    for query in insert_table_queries:
        print('\nInserting data into table ► ' + query.split()[2])
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to insert data into all tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('\nConection to the cluster sucessfull')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('\n\n\t\tData sucessfully inserted into tables')
    
    conn.close()


if __name__ == "__main__":
    main()