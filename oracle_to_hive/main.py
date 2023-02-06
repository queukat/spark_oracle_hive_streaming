# importing necessary libraries
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main(args):
    spark = SparkSession.builder().appName("OracleToHive").getOrCreate()
    jdbcUrl = "jdbc:oracle:thin:@hostname:port/service_name"
    jdbcTable = "oracle_table"
    jdbcUser = "username"
    jdbcPassword = "password"
    hiveTable = "hive_table"
    hiveDatabase = "hive_database"
    hiveFormat = "orc"
    hivePartitionColumns = ["column1", "column2"]
    hivePartitionValues = ["value1", "value2"]
    hivePartitionLocation = "hdfs:///hive/warehouse/hive_database.db/hive_table"
    streamingInterval = 10  # seconds

    # Get Oracle Table Schema
    oracleTableSchema = spark.read.format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", jdbcTable) \
        .option("user", jdbcUser) \
        .option("password", jdbcPassword) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load() \
        .schema

    # Cast Oracle Column Data Types
    castedSchema = [StructField(field.name,
                                StringType if field.dataType == types.StringType else
                                DecimalType(38, 0) if field.dataType == types.IntegerType else
                                TimestampType if field.dataType == types.DateType else
                                field.dataType,
                                field.nullable, field.metadata) for field in oracleTableSchema]

    # Download Table in Hive with ORC Format
    downloadedDF = spark.read.format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", jdbcTable) \
        .option("user", jdbcUser) \
        .option("password", jdbcPassword) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    castedDF = downloadedDF.selectExpr([f"cast({col} as {field.dataType}) as {field.name}"
                                        for col, field in zip(downloadedDF.columns, castedSchema)])

    downloadedDF.write \
        .format(hiveFormat) \
        .mode("overwrite") \
        .partitionBy(*hivePartitionColumns) \
        .saveAsTable(f"{hiveDatabase}.{hiveTable}")

    # Change Data Capture of Oracle Table using Spark Streaming from JDBC
    streamingDF = spark.readStream \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", jdbcTable) \
        .option("user", jdbcUser) \
        .option("password", jdbcPassword) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load() \
        .selectExpr([f"cast({col} as {field.dataType}) as {field.name}"
                     for col, field in zip(downloadedDF.columns, castedSchema)])

    streamingQuery = streamingDF.writeStream \
        .format(hiveFormat) \
        .option("checkpointLocation", hivePartitionLocation) \
        .outputMode("append") \
        .partitionBy(*hivePartitionColumns) \
        .start(f"{hivePartitionLocation}/{'/'.join(hivePartitionValues)}")

    streamingQuery.awaitTermination(streamingInterval * 1000)


if __name__ == "__main__":
    main(sys.argv)
