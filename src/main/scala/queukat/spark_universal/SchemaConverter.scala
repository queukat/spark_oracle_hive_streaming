package queukat.spark_universal

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import queukat.spark_universal.models.ColumnInfo

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.BigDecimal.javaBigDecimal2bigDecimal
//The class is responsible for converting the schema of the data obtained from the database.
class SchemaConverter(spark: SparkSession, owner: String, tableName: String, skipTypeCheck: Boolean, dbReader: DbReader) {

  import spark.implicits._
  //Method to convert column info to struct field

  def convertColumnInfoToStructFieldAsync(info: ColumnInfo): Future[StructField] = Future {
    if (skipTypeCheck) {
      val originalDataType = info.dataType match {
        case "VARCHAR2" => StringType
        case "DATE" => TimestampType
        case "NUMBER" =>
          val precision = if (info.dataPrecision != null) info.dataPrecision.toInt else 38
          val scale = if (info.dataScale != null) info.dataScale.toInt else 0
          DecimalType(precision, scale)
        case "FLOAT" => FloatType
        case "DOUBLE" => DoubleType
        case "INT" => IntegerType
        case "BOOLEAN" => BooleanType
        case _ => StringType
      }
      StructField(info.columnName, originalDataType, nullable = true)
    } else {
      val hiveDataType = info.dataType match {
        case "VARCHAR2" => StringType
        case "DATE" => TimestampType
        case "NUMBER" =>
          if (info.dataPrecision == null || info.dataScale == null) {
            val maxLeftOfDecimalQuery = s"""select max(abs(trunc(${info. columnName},0))) from "$owner"."$tableName""""
            val maxRightOfDecimalQuery = s"""select max(mod(${info.columnName}, 1)) from "$owner"."$tableName""""
            val maxLeftOfDecimal = {
              val result = dbReader.readFromJDBC(maxLeftOfDecimalQuery).first().getDecimal(0)
              val strResult = result.toBigInt().toString()
              strResult.length
            }
            val maxRightOfDecimal = {
              val result = dbReader.readFromJDBC(maxRightOfDecimalQuery).first().getDecimal(0)
              val strResult = result.stripTrailingZeros().toPlainString()
              if (strResult.contains(".")) strResult.substring(strResult.indexOf(".") + 1).length else 0
            }
            if (maxLeftOfDecimal + maxRightOfDecimal > 38) {
              StringType
            } else {
              DecimalType(maxLeftOfDecimal, maxRightOfDecimal)
            }
          } else {
            DecimalType(info.dataPrecision.toInt, info.dataScale.toInt)
          }
        case _ => StringType
      }
      StructField(info.columnName, hiveDataType, nullable = true)
    }
  }
  //Метод преобразует схему Oracle в схему Spark.
  def convert(oracleSchema: DataFrame): Future[Seq[StructField]] = {
    val castedSchema = oracleSchema
      .select(
        col("COLUMN_NAME").alias("columnName"),
        col("DATA_TYPE").alias("dataType"),
        col("DATA_PRECISION").cast(StringType).alias("dataPrecision"),
        col("DATA_SCALE").cast(StringType).alias("dataScale")
      )
      .as[ColumnInfo]

    Future.sequence {
      castedSchema.collect().map(convertColumnInfoToStructFieldAsync)
    }
  }

}
