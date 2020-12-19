import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops each table using the queries in `drop_table_queries` list.
    
        Parameters:
            cur: A cursor used to execute SQL commands over an open connection to a Redshift database.
            conn: An open connection to a Redshift database. Used to commit changes.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates each table using the queries in `create_table_queries` list.
    
        Parameters:
            cur: A cursor used to execute SQL commands over an open connection to a Redshift database.
            conn: An open connection to a Redshift database. Used to commit changes.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Uses config credentials to connect to Redshift database, drops all tables if they exist, 
        creates all tables as implemented in `sql_queries.py`, then finally closes the connection to the database.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()