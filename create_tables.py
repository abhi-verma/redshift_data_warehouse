import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drops the staging, fact and dimensional tables (if they exist) using the drop_table_queries list imported from
        sql_queries.py file
        :param cur: cursor variable to access the db
        :param conn: connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Creates the staging, fact and dimensional tables (if they don't exist) using the create_table_queries list
        imported from sql_queries.py file
        :param cur: cursor variable to access the db
        :param conn: connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Parses the dwh.cfg file to get parameters for Redshift cluster, database name, user, password and port
        Creates a connection to the Redshift cluster by using the parameters
        Drops and re-creates the staging, fact and dimensional tables
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