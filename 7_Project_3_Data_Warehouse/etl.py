import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to insert data into staging tables and also displays the number of rows inserted in each table
    
    inputs:
    cur ► cursor to execute instructions
    conn ► the connection to the cluster
    """
    
    print('\n\t\tInserting data into staging tables ...')
    
    for query in copy_table_queries:
        print('\nInserting data into table ► ' + query.split()[1])
        cur.execute(query)
        cur.execute("SELECT COUNT(*) FROM {}".format(query.split()[1]))
        print("Number of rows inserted: ", cur.fetchone()[0])
        conn.commit()



def insert_tables(cur, conn):
    """
    Function to insert data into the schema tables and also displays the number of rows inserted in each table
    
    inputs:
    cur ► cursor to execute instructions
    conn ► the connection to the cluster
    """
    
    print('\n\n\t\tInserting data into schema tables ...')
    
    for query in insert_table_queries:
        print('\nInserting data into table ► ' + query.split()[2])
        cur.execute(query)
        cur.execute("SELECT COUNT(*) FROM {}".format(query.split()[2]))
        print("Number of rows inserted: ", cur.fetchone()[0])
        conn.commit()


def main():
    """
    Main function to call the other functions and insert data into all tables
    It takes all the credentials and makes the connection to the cluster
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