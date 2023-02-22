import psycopg2
from psycopg2._psycopg import connection

def create_table(conn: connection, table: str, schema: str):
    """ creates a table in the PostgreSQL database"""
    command ="""CREATE TABLE IF NOT EXISTS {} ({})""".format(table,schema)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("create table command = ",command)
        cur.execute(f'{command}')
        print("Table {} created... ".format(table))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def create_view(conn: connection, view: str, query: str):
    """ creates a view in the PostgreSQL database"""
    command ="""CREATE OR REPLACE VIEW {} as {}""".format(view,query)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("create view command = ",command)
        cur.execute(f'{command}')
        print("View {} created... ".format(view))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def insert_values_from_csv(conn: connection, table: str, schema: str, csvfile_path: str):
    # TO DO : remove from hard code delimiter and header option ; to include as input parameters
    """ inserts values from a csv file in a PostgreSQL table"""
    command = """COPY {}({}) FROM '{}' DELIMITER ',' CSV HEADER;""".format(table,schema,csvfile_path)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("insert values from csv command = ",command)
        cur.execute(f'{command}')
        print("Values inserted in table {}... ".format(table))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def alter_table(conn: connection, table: str, query: str):
    """ alters a PostgreSQL table"""
    command ="""ALTER TABLE {} {}""".format(table,query)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("alter table command = ",command)
        cur.execute(f'{command}')
        print("Table {} modified... ".format(table))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def update_table(conn: connection, table: str, col_val: str, query: str = ""):
    """ update a PostgreSQL table"""
    command ="""UPDATE {} SET {} {}""".format(table,col_val,query)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("update table command = ",command)
        cur.execute(f'{command}')
        print("Update {} done... ".format(table))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def select_from_table(conn: connection, table: str, query: str = "", columns: str = "*"):
    """ selects data from a PostgreSQL table"""
    command ="""SELECT {} from {} {}""".format(columns,table,query)

    try:
        # create a cursor
        cur = conn.cursor()
        
        #print("select from table command = ",command)
        cur.execute(f'{command}')
        print("Select from {} done... ".format(table))
        query_result=cur.fetchall()
        #print(query_result)
        cur.close()
        return query_result
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def drop_table(conn: connection, table: str):
    """ drops a table in the PostgreSQL database"""
    command = """DROP TABLE {}""".format(table)

    try:
        # create a cursor
        cur = conn.cursor()

        #print("drop table command = ",command)
        cur.execute(f'{command}')
        print("Table {} dropped... ".format(table))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def drop_view(conn: connection, view: str):
    """ drops a view in the PostgreSQL database"""
    command = """DROP VIEW IF EXISTS {} CASCADE;""".format(view)

    try:
        # create a cursor
        cur = conn.cursor()

        #print("drop view command = ",command)
        cur.execute(f'{command}')
        print("View {} dropped... ".format(view))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def delete_from_table(conn: connection, table: str, query: str = ""):
    """ delete rows from a PostgreSQL table"""
    command ="""DELETE FROM {} {}""".format(table,query)

    try:
        # create a cursor
        cur = conn.cursor()
        
        cur.execute(f'{command}')
        print("Delete from {} {} done... ".format(table, query))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)