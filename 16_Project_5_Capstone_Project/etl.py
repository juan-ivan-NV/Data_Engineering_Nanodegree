import configparser
import psycopg2
from sql_queries import copy_table_queries


def s3_to_redshift(cur, conn):
    """
    Function to insert data into the tables and also displays the number of rows inserted in each table
    
    Args:
        cur (:obj:`psycopg2.extensions.cursor`) ► Cursor for connection
        conn (:obj:`psycopg2.extensions.connection`) ► database connection
    """
    
    print('\n\t\tInserting data into staging tables ...')
    
    for query in copy_table_queries:
        print('\nInserting data from S3 to redshift table ► ' + query.split()[1])
        print(query)
        cur.execute(query)
        cur.execute("SELECT COUNT(*) FROM {}".format(query.split()[1]))
        print("Number of rows inserted: ", cur.fetchone()[0])
        conn.commit()
        
def main():
    """
    Main function to call the function s3_to_redshift and populate the tables
    It takes all the credentials and makes the connection to the cluster
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('\nConection to the cluster sucessfull')
    
    s3_to_redshift(cur, conn)
    print('\n\n\t\tData sucessfully inserted into tables')
    
    conn.close()


if __name__ == "__main__":
    main()