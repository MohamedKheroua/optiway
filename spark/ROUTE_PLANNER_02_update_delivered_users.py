#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession

# you need these two to transform the json strings to dataframes
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

import os


# In[ ]:


from modules.postgresql.postgres_connection import connect, close_connection
from modules.postgresql.tables import *


# In[ ]:


# Spark session & context
spark = (SparkSession
         .builder
         .master('local[8]')
         .appName('Kafka PySpark - Update PostgreSQL table users')
         # Versions need to match the Spark version (trial & error)
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         # PostgreSQL package
         # ERROR SparkContext: Failed to add /opt/spark/jars/postgresql-42.2.5.jar to Spark environment
         #.config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar")
         .config("spark.jars", "/usr/local/spark-2.4.5-bin-hadoop2.7/jars/postgresql-42.2.5.jar")
         .getOrCreate())
sc = spark.sparkContext


# In[ ]:


#get the name of the table to load
#load the user to update table in PostgreSQL
df_table_name = spark.read     .format("jdbc")     .option("url", "jdbc:postgresql://host.docker.internal:5432/"+os.environ["PGDATABASE"])     .option("dbtable", "tables_to_update")     .option("user", "postgres")     .option("password", os.environ["PGPASSWORD"])     .option("driver", "org.postgresql.Driver")     .load()


# In[ ]:


# process all the table of delivered users

conn = connect()

email_list = []
for table_name in df_table_name.collect():
    temp_email_listDF = spark.read         .format("jdbc")         .option("url", "jdbc:postgresql://host.docker.internal:5432/"+os.environ["PGDATABASE"])         .option("dbtable", table_name['name'])         .option("user", "postgres")         .option("password", os.environ["PGPASSWORD"])         .option("driver", "org.postgresql.Driver")         .load()
    
    email_list = [row['email'] for row in temp_email_listDF.collect()]
    
    # Spark SQL doesn't have an equivalent of the PostgreSQL UPDATE ou DELETE where
    # update users table directly with custom function update_table from postgresql module
    # set a_livrer field to 'non' (users have been delivered)
    update_table(
        conn=conn,
        col_val="""a_livrer = 'non'""",
        table="users",
        query="""WHERE (email in ({}))""".format(str(email_list)[1:-1])
    )
    
    # delete the row corresponding to table_name['name'] from tables_to_update (as its users have been processed)
    delete_from_table(
        conn=conn,
        table='tables_to_update',
        query= "where name = '{}'".format(table_name['name'])
    )
    
    # drop the table table_name['name']
    drop_table(
        conn=conn,
        table='{}'.format(table_name['name'])
    )

close_connection(conn=conn)

