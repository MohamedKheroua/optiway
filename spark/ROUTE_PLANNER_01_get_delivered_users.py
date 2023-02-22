#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession

# you need these two to transform the json strings to dataframes
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json

import os
import time


# In[ ]:


# Spark session & context
spark = (SparkSession
         .builder
         .master('local[8]')
         .appName('Kafka PySpark - Overwrite PostgreSQL table users_to_update')
         # Versions need to match the Spark version (trial & error)
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         # PostgreSQL package
         # ERROR SparkContext: Failed to add /opt/spark/jars/postgresql-42.2.5.jar to Spark environment
         #.config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar")
         .config("spark.jars", "/usr/local/spark-2.4.5-bin-hadoop2.7/jars/postgresql-42.2.5.jar")
         .getOrCreate())
sc = spark.sparkContext


# In[ ]:


# Read the message from the kafka stream
df_kafka_input_raw_data = spark   .readStream   .format("kafka")   .option("kafka.bootstrap.servers", "kafka:9092")   .option("subscribe", "get-delivery-adresses-topic")   .option("failOnDataLoss", "false")   .load()

# convert the binary values to string
df_kafka_input_coverted_to_str = df_kafka_input_raw_data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# In[ ]:


#Create a small temporary view for SparkSQL
df_kafka_input_coverted_to_str.createOrReplaceTempView("message")


# In[ ]:


# Write out the message to the console of the environment
res = spark.sql("SELECT * from message")
res.writeStream.format("console")             .outputMode("append")             .start() 


# In[ ]:


from pyspark.sql import Row

# Keep records of the temp table name, in order to access it in another script, to update the delivered users 
temp_table_name = "temp_delivered_users_"+time.strftime("%Y%m%d_%H%M%S")

tables_to_updateDF = spark.createDataFrame([Row(name=temp_table_name)])

tables_to_updateDF.write         .format("jdbc")         .option("url", "jdbc:postgresql://host.docker.internal:5432/"+os.environ["PGDATABASE"])         .option("dbtable", "tables_to_update")         .option("user", "postgres")         .option("password", os.environ["PGPASSWORD"])         .option("driver", "org.postgresql.Driver")         .mode("append")         .save()


# In[ ]:


# Write the message into a temporary table in PostgreSQL
def foreach_batch_function(df,epoch_id,temp_table_name=temp_table_name):
    # Transform and write batchDF in this foreach
    
    #Transform the values of all rows in column value and create a dataframe out of it (will also only have one row)
    df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))    
   
    # Transform the dataframe so that it will have individual columns 
    df3= df2.select(["value.email"])
    
    # Send the dataframe into PostgreSQL which will save the data into the table users_pyspark_test
    df3.write         .format("jdbc")         .option("url", "jdbc:postgresql://host.docker.internal:5432/"+os.environ["PGDATABASE"])         .option("dbtable", temp_table_name)         .option("user", "postgres")         .option("password", os.environ["PGPASSWORD"])         .option("driver", "org.postgresql.Driver")         .mode("append")         .save()
    
    pass


# In[ ]:


# Start the PostgreSQL stream and wait for termination
df_kafka_input_coverted_to_str.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

