#!/usr/bin/python
from configparser import ConfigParser
from pathlib import Path
import os
import pathlib
import psycopg2
from psycopg2._psycopg import connection

def config(filename=str(pathlib.Path(__file__).parent.resolve()).replace("\\","/")+"/database.ini", section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = os.environ.get(param[1])
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        #print('params = ',params)

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        #print("conn = ",conn)

        #Setting auto commit false
        conn.autocommit = True

    #     # create a cursor
    #     cur = conn.cursor()
        
	# # execute a statement
    #     print('PostgreSQL database version:')
    #     cur.execute('SELECT version()')

    #     # display the PostgreSQL database server version
    #     db_version = cur.fetchone()
    #     print(db_version)
       
	# # close the communication with the PostgreSQL
    #     cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()
    #         print('Database connection closed.')

    return conn

def close_connection(conn: connection):
    """ Close connexion to the PostgreSQL database server """
    try:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)