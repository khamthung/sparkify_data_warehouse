import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, analytic_queries
from prettytable import from_db_cursor


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def test_analytic_queries(cur, conn):
    for query in analytic_queries:
        cur.execute(query)
        conn.commit()
        table = from_db_cursor(cur) 
        print(table)


        # results = cur.fetchall()
        # print("\n",cur.description)
        # for row in results:
        #     print (row)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    test_analytic_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()