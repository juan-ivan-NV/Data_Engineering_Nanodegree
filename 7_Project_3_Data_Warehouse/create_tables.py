import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Funciton to execute the drop tables queries
    It receives cursor and connection variables
    """
    
    for query in drop_table_queries:
        print('Deleting table')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Funciton to execute the create tables queries
    It receives cursor and connection variables
    """
    for query in create_table_queries:
        print("\nRunning" + query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to connect to the cluster, reset the DB, and then create the tables 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Cluster connection succesfull')

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print('Tables created')

    conn.close()


if __name__ == "__main__":
    main()