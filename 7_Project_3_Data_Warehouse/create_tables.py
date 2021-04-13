import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Funciton to execute the drop tables queries and also shows the table affected
    
    Inputs:
    conn variable ► the connection to the database
    cur variable ► to execute the query 
    """
    
    print('\nDroping tables ...')
    
    for query in drop_table_queries:
        print('\nIf exists table "{}" is deleted'.format(query.split()[-1]))
        cur.execute(query)
        conn.commit()
        
    print('\n\t\tAll tables successfully deleted')
    

def create_tables(cur, conn):
    """
    Funciton to execute the create tables queries and display the name of the table
    
    Inputs:
    conn variable ► the connection to the database
    cur variable ► to execute the query 
    """
    
    print('\n\nCreating tables ...')
    
    for query in create_table_queries:
        print("\nCreating table ► " + query.split()[5])
        cur.execute(query)
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

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print('\n\n\t\tAll tables successfully created\n\n')

    conn.close()


if __name__ == "__main__":
    main()