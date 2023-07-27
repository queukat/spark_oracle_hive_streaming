package queukat.spark_universal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import queukat.spark_universal.models.ColumnInfo

import scala.math.BigDecimal.javaBigDecimal2bigDecimal

/**
 * Converts the schema of the data obtained from the database.
 *
 * @constructor creates a new SchemaConverter with a spark session, owner name, table name, type check flag and a DbReader.
 * @param spark the spark session used to perform transformations.
 * @param owner the owner of the table.
 * @param tableName the table to be processed.
 * @param typeCheck flag to determine the type check strategy.
 * @param dbReader the DbReader used to read data.
 */
class SchemaConverter(
                       spark: SparkSession,
                       owner: String,
                       tableName: String,
                       typeCheck: String,
                       dbReader: DbReader
                     ) {
  import spark.implicits._

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Extracts column information from an Oracle schema.
   *
   * The method takes a DataFrame that represents an Oracle schema,
   * selects and renames the relevant columns and converts them into
   * a sequence of [[ColumnInfo]] objects, each representing a column in the schema.
   *
   * @param oracleSchema the DataFrame representing the Oracle schema
   * @return a sequence of ColumnInfo objects, each representing a column in the schema
   */
  def getColumnInfo(oracleSchema: DataFrame): Seq[ColumnInfo] = {
    logger.info("Getting column information from the Oracle schema.")
    val castedSchema = oracleSchema.select(
      col("COLUMN_NAME").alias("columnName"),
      col("DATA_TYPE").alias("dataType"),
      col("DATA_PRECISION").cast(StringType).alias("dataPrecision"),
      col("DATA_SCALE").cast(StringType).alias("dataScale")).as[ColumnInfo]

    castedSchema.collect().toList
  }
  /**
   * Converts an Oracle schema DataFrame to a sequence of Spark StructFields.
   *
   * The method begins by converting the DataFrame containing the Oracle schema into a sequence of [[ColumnInfo]] objects.
   * Each [[ColumnInfo]] object is then converted into a [[StructField]].
   * Depending on the [[typeCheck]] flag, the schema may be further analyzed and converted.
   *
   * @param oracleSchema The DataFrame containing the Oracle schema to be converted.
   * @return A sequence of StructFields representing the converted schema.
   * @throws IllegalArgumentException if the provided typeCheck value is invalid.
   */

  def convert(oracleSchema: DataFrame): Seq[StructField] = {
    logger.info("Starting schema conversion.")
    val columnInfos = getColumnInfo(oracleSchema)
    val fields = columnInfos.map(info => convertColumnInfoToStructField(info))

    if (typeCheck == "spark") {
      val tempDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], new StructType(fields.toArray))
      analyzeAndConvertSchema(tempDF)
    } else {
      fields
    }
  }

  /**
   * Converts a ColumnInfo object to a Spark StructField.
   *
   * The method takes a [[ColumnInfo]] object, which represents a column in an Oracle schema,
   * and converts it into a [[StructField]], which represents a column in a Spark schema.
   * The conversion process involves determining the appropriate Spark data type based on the Oracle data type,
   * and setting the nullable property to true.
   *
   * @param info a ColumnInfo object
   * @return a StructField object representing the converted column
   * @throws Exception if the conversion fails
   */
  def convertColumnInfoToStructField(info: ColumnInfo): StructField =  {
    logger.info(s"Converting column information to StructField for column: ${info.columnName}")
    try {
      val (dataType, metadata) = info.dataType match {
        case "VARCHAR2" => (StringType, new MetadataBuilder().build())
        case "DATE" => (TimestampType, new MetadataBuilder().build())
        case "NUMBER" => typeCheck match {
          case "skip" => val precision = if (info.dataPrecision != null) info.dataPrecision.toDouble.toInt else 38
            val scale = if (info.dataScale != null) info.dataScale.toDouble.toInt else 0
            (DecimalType(precision, scale), new MetadataBuilder().build())
          case "oracle" => if (info.dataPrecision == null || info.dataScale == null) {
            val maxLeftOfDecimalQuery = s"""select max(abs(trunc(${info.columnName},0))) from "$owner"."$tableName""""
            val maxRightOfDecimalQuery = s"""select max(mod(${info.columnName}, 1)) from "$owner"."$tableName""""
            val maxLeftOfDecimal = {
              val result = dbReader.readFromJDBC(maxLeftOfDecimalQuery).first().getDecimal(0)
              val strResult = result.toBigInt.toString()
              strResult.length
            }
            val maxRightOfDecimal = {
              val result = dbReader.readFromJDBC(maxRightOfDecimalQuery).first().getDecimal(0)
              val strResult = result.stripTrailingZeros().toPlainString()
              if (strResult.contains(".")) strResult.substring(strResult.indexOf(".") + 1).length else 0
            }
            if (maxLeftOfDecimal + maxRightOfDecimal > 38) {
              (StringType, new MetadataBuilder().build())
            } else {
              (DecimalType(maxLeftOfDecimal, maxRightOfDecimal), new MetadataBuilder().build())
            }
          } else {
            (DecimalType(info.dataPrecision.toDouble.toInt, info.dataScale.toDouble.toInt), new MetadataBuilder().build())
          }
          case "spark" => if (info.dataPrecision == null || info.dataScale == null) (StringType, new MetadataBuilder().putString("toAnalyze", "true").build()) else (DecimalType(info.dataPrecision.toDouble.toInt, info.dataScale.toDouble.toInt), new MetadataBuilder().build())
          case _ => throw new IllegalArgumentException(s"Invalid typeCheck value: $typeCheck")
        }
        case "FLOAT" => (FloatType, new MetadataBuilder().build())
        case "DOUBLE" => (DoubleType, new MetadataBuilder().build())
        case "INT" => (IntegerType, new MetadataBuilder().build())
        case "BOOLEAN" => (BooleanType, new MetadataBuilder().build())
        case _ => (StringType, new MetadataBuilder().build())
      }
      StructField(info.columnName, dataType, nullable = true, metadata)
    } catch {
      case e: Exception => logger.error(s"Failed to convert column info to struct field due to ${e.getMessage}")
        throw e
    }
  }

  /**
   * Analyzes and converts a DataFrame's schema.
   *
   * The method iterates through the fields in the DataFrame's schema,
   * and if the field's data type is StringType and the "toAnalyze" flag in its metadata is set to "true",
   * it calculates the maximum number of digits to the left and right of the decimal point for the values in the column,
   * and if the sum of these numbers is greater than 38, it leaves the data type as StringType, otherwise, it changes the data type to DecimalType.
   *
   * @param df a DataFrame with the schema to analyze and convert
   * @return a sequence of StructFields representing the converted schema
   */
  def analyzeAndConvertSchema(df: DataFrame): Seq[StructField] = {
    logger.info("Starting schema analysis and conversion.")
    df.schema.map { field =>
      if (field.dataType == StringType && field.metadata.getString("toAnalyze") == "true") {
        val maxLeftOfDecimal = df.select(abs(floor(df(field.name)))).agg(max("value")).first().getDouble(0).toString.length
        val maxRightOfDecimal = df.select(abs(df(field.name) - floor(df(field.name)))).agg(max("value")).first().getDouble(0).toString.split("\\.").lastOption.getOrElse("").length
        if (maxLeftOfDecimal + maxRightOfDecimal > 38) {
          field.copy(dataType = StringType)
        } else {
          field.copy(dataType = DecimalType(maxLeftOfDecimal, maxRightOfDecimal))
        }
      } else {
        field
      }
    }
  }
}
