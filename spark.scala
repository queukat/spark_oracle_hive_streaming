import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object OracleToHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OracleToHive").getOrCreate()
    val oracleTableName = args(0)
    val hiveTableName = args(1)
    val oracleUrl = args(2)
    val hiveUrl = args(3)
    val oracleUser = args(4)
    val oraclePassword = args(5)
    val hiveUser = args(6)
    val hivePassword = args(7)
    
    // Get Oracle Table Columns and Data Types
    val oracleTableSchema = spark.read.format("jdbc").option("url", oracleUrl).option("dbtable", oracleTableName).option("user", oracleUser).option("password", oraclePassword).option("driver", "oracle.jdbc.driver.OracleDriver").load().schema
    
    // Cast Oracle Column Data Types
    val castedSchema = oracleTableSchema.map(field => {
      if (field.dataType == IntegerType) {
        StructField(field.name, DecimalType(38, 0), field.nullable, field.metadata)
      } else if (field.dataType == StringType) {
        StructField(field.name, StringType, field.nullable, field.metadata)
      } else if (field.dataType == DateType) {
        StructField(field.name, TimestampType, field.nullable, field.metadata)
      } else {
        field
      }
    })
    
    // Download Table in Hive with ORC Format
    val oracleTableDF = spark.read.format("jdbc").option("url", oracleUrl).option("dbtable", oracleTableName).option("user", oracleUser).option("password", oraclePassword).option("driver", "oracle.jdbc.driver.OracleDriver").load()
    oracleTableDF.write.format("orc").mode("overwrite").option("url", hiveUrl).option("dbtable", hiveTableName).option("user", hiveUser).option("password", hivePassword).save()
    
    // Change Data Capture of Oracle Table using Spark Streaming
    val streamingDF = spark.readStream.format("jdbc").option("url", oracleUrl).option("dbtable", oracleTableName).option("user", oracleUser).option("password", oraclePassword).option("driver", "oracle.jdbc.driver.OracleDriver").load()
    val streamingQuery = streamingDF.writeStream.format("orc").option("url", hiveUrl).option("dbtable", hiveTableName).option("user", hiveUser).option("password", hivePassword).option("checkpointLocation", "/tmp/checkpoint").start()
    
    // Append Streaming Data to Hive Table in ORC Format
    streamingQuery.awaitTermination()
  }
}
