#importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import DataStreamWriter

#creating spark session
spark = SparkSession.builder.appName("OracleToHive").getOrCreate()

#creating oracle connection
oracle_url = "jdbc:oracle:thin:@localhost:1521:orcl"
oracle_properties = dict(user="username", password="password", driver="oracle.jdbc.driver.OracleDriver")

#creating hive connection
hive_url = "jdbc:hive2://localhost:10000/default"
hive_properties = dict(user="username", password="password", driver="org.apache.hive.jdbc.HiveDriver")

#creating oracle table
oracle_table = spark.read.jdbc(url=oracle_url, table="oracle_table", properties=oracle_properties)

#creating hive table
hive_table = spark.read.jdbc(url=hive_url, table="hive_table", properties=hive_properties)

#creating mapping for oracle data types
oracle_data_types_map = dict(NUMBER="DECIMAL", VARCHAR2="STRING", DATE="TIMESTAMP")

#creating schema for oracle table
oracle_schema = StructType([StructField(c.name, oracle_data_types_map.get(c.dataType, c.dataType), True) for c in oracle_table.schema.fields])

#downloading oracle table in hive with orc format
oracle_table.write.format("orc").mode("overwrite").saveAsTable("hive_table")

#creating streaming query
streaming_query = DataStreamWriter(oracle_table.writeStream.format("orc").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/streaming"))

#appending streaming data to hive table
streaming_query.foreachBatch(lambda df, epoch_id: df.write.format("orc").mode("append").saveAsTable("hive_table"))

#stopping streaming query
streaming_query.stop()

#stopping spark session
spark.stop()
