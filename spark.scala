import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark._
import java.sql.Timestamp

object OracleToHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OracleToHive").getOrCreate()
    val jdbcUrl = "jdbc:oracle:thin:@hostname:port/service_name"
    val jdbcTable = "oracle_table"
    val jdbcUser = "username"
    val jdbcPassword = "password"
    val hiveTable = "hive_table"
    val hiveDatabase = "hive_database"
    val hiveFormat = "orc"
    val hivePartitionColumns = Seq("column1", "column2")
    val hivePartitionValues = Seq("value1", "value2")
    val hivePartitionLocation = "hdfs:///hive/warehouse/hive_database.db/hive_table"
    val streamingInterval = 10 //seconds

    //Get Oracle Table Schema
    val oracleTableSchema = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
      .schema

    //Cast Oracle Column Data Types
    val castedSchema = oracleTableSchema.map(field => {
      field.dataType match {
        case StringType => StructField(field.name, StringType, field.nullable, field.metadata)
        case IntegerType => StructField(field.name, DecimalType(38, 0), field.nullable, field.metadata)
        case DateType => StructField(field.name, TimestampType, field.nullable, field.metadata)
        case _ => field
      }
    })

//Download Table in Hive with ORC Format
    val downloadedDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()

    val castedDF = downloadedDF.select(downloadedDF.columns.map(col => colcast(col, castedSchema.find(_.name == col).get.dataType)): _*)

    castedDF.write
      .format(hiveFormat)
      .mode("overwrite")
      .partitionBy(hivePartitionColumns:_*)
      .saveAsTable(s"$hiveDatabase.$hiveTable")

    //Change Data Capture of Oracle Table using Spark Streaming from JDBC
    val streamingDF = spark.readStream
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
      .schema(castedSchema)


    val streamingQuery = streamingDF.writeStream
      .format(hiveFormat)
      .option("checkpointLocation", hivePartitionLocation)
      .outputMode("append")
      .partitionBy(hivePartitionColumns:_*)
      .start(hiveDatabase + "." + hiveTable)

    streamingQuery.awaitTermination(streamingInterval * 1000)
  }
}
