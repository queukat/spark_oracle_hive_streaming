package queukat.spark_universal

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import queukat.spark_universal.models.ColumnInfo

/**
 * Converts Oracle schema metadata into Spark schema and applies it to DataFrames.
 */
class SchemaConverter(
  spark: SparkSession,
  owner: String,
  tableName: String,
  typeCheck: String,
  dbReader: DbReader,
  snapshotScn: Option[Long]
) {
  import spark.implicits._

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getColumnInfo(oracleSchema: DataFrame): Seq[ColumnInfo] = {
    logger.info("Getting column information from the Oracle schema.")
    oracleSchema.select(
      col("COLUMN_NAME").alias("columnName"),
      col("DATA_TYPE").alias("dataType"),
      col("DATA_PRECISION").cast(StringType).alias("dataPrecision"),
      col("DATA_SCALE").cast(StringType).alias("dataScale")
    ).as[ColumnInfo].collect().toList
  }

  def convert(oracleSchema: DataFrame): Seq[StructField] = {
    logger.info("Starting schema conversion.")
    getColumnInfo(oracleSchema).map(convertColumnInfoToStructField)
  }

  def castToSchema(df: DataFrame, targetSchema: StructType): DataFrame = {
    val missingColumns = targetSchema.fieldNames.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Source DataFrame is missing columns required by the target schema: ${missingColumns.mkString(", ")}")
    }

    val projectedColumns = targetSchema.fields.map { field =>
      col(field.name).cast(field.dataType).alias(field.name)
    }

    df.select(projectedColumns: _*)
  }

  def convertColumnInfoToStructField(info: ColumnInfo): StructField = {
    logger.info(s"Converting column information to StructField for column: ${info.columnName}")
    val dataType = normalizedDataType(info) match {
      case "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" | "CLOB" | "NCLOB" | "LONG" => StringType
      case "DATE" | "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH LOCAL TIME ZONE" => TimestampType
      case "NUMBER" => resolveNumberType(info)
      case "FLOAT" | "BINARY_FLOAT" => FloatType
      case "DOUBLE" | "BINARY_DOUBLE" => DoubleType
      case "INT" | "INTEGER" => IntegerType
      case "BOOLEAN" => BooleanType
      case "RAW" | "LONG RAW" | "BLOB" => BinaryType
      case unsupported => throw new UnsupportedOperationException(s"Unsupported Oracle data type for migration: $unsupported")
    }
    StructField(info.columnName, dataType, nullable = true)
  }

  private def normalizedDataType(info: ColumnInfo): String = {
    Option(info.dataType).map(_.trim.toUpperCase).getOrElse {
      throw new IllegalArgumentException(s"Column ${info.columnName} has no Oracle data type in ALL_TAB_COLUMNS.")
    }
  }

  private def resolveNumberType(info: ColumnInfo): DataType = {
    val precision = Option(info.dataPrecision).map(_.toInt)
    val scale = Option(info.dataScale).map(_.toInt)

    (precision, scale) match {
      case (Some(p), Some(s)) => decimalTypeFor(p, s)
      case _ =>
        typeCheck match {
          case "spark" => StringType
          case "skip" => StringType
          case "oracle" => inferNumberType(info.columnName)
          case other => throw new IllegalArgumentException(s"Invalid typeCheck value: $other")
        }
    }
  }

  private def decimalTypeFor(precision: Int, scale: Int): DecimalType = {
    val normalizedScale = math.max(scale, 0)
    val widenedPrecision = precision + math.max(-scale, 0)
    val normalizedPrecision = math.min(38, math.max(normalizedScale + 1, widenedPrecision))
    DecimalType(normalizedPrecision, normalizedScale)
  }

  private def inferNumberType(columnName: String): DataType = {
    val query = QueryGenerator.generateNumberProfileQuery(owner, tableName, columnName, snapshotScn)
    val profileRow = dbReader.readFromJDBC(query).first()
    val leftDigits = extractInt(profileRow.get(0), defaultValue = 1)
    val rightDigits = extractInt(profileRow.get(1), defaultValue = 0)
    val precision = leftDigits + rightDigits

    if (precision <= 0 || precision > 38) {
      logger.warn(s"Oracle NUMBER column $columnName exceeds Spark decimal precision or could not be profiled safely. Falling back to StringType.")
      StringType
    } else {
      DecimalType(precision, math.max(0, rightDigits))
    }
  }

  private def extractInt(value: Any, defaultValue: Int): Int = {
    Option(value).map {
      case n: java.math.BigDecimal => n.intValue()
      case n: java.lang.Number => n.intValue()
      case s: String => s.toInt
      case other => other.toString.toInt
    }.getOrElse(defaultValue)
  }
}
