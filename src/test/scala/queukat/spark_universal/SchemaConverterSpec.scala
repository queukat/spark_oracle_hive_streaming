package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import queukat.spark_universal.models.ColumnInfo

class SchemaConverterSpec extends AnyFunSuite with SparkTestSession {
  private class StubDbReader(profile: DataFrame) extends DbReader(
    spark,
    "jdbc:oracle:thin:@//localhost:1521/ORCL",
    "user",
    "password",
    numPartitions = 1,
    fetchSize = 1000
  ) {
    override def readFromJDBC(query: String): DataFrame = profile
  }

  test("maps supported Oracle types to Spark types") {
    val localSpark = spark
    import localSpark.implicits._

    val profile = Seq((BigDecimal(10).bigDecimal, BigDecimal(2).bigDecimal)).toDF("LEFT_DIGITS", "RIGHT_DIGITS")
    val converter = new SchemaConverter(localSpark, "OWNER", "TABLE", "skip", new StubDbReader(profile), None)

    assert(converter.convertColumnInfoToStructField(ColumnInfo("TXT", "VARCHAR2", null, null)).dataType == StringType)
    assert(converter.convertColumnInfoToStructField(ColumnInfo("TS", "TIMESTAMP WITH TIME ZONE", null, null)).dataType == TimestampType)
    assert(converter.convertColumnInfoToStructField(ColumnInfo("RAW_COL", "RAW", null, null)).dataType == BinaryType)
    assert(converter.convertColumnInfoToStructField(ColumnInfo("NUM", "NUMBER", "10", "2")).dataType == DecimalType(10, 2))
  }

  test("widens negative scale NUMBER to a safe decimal") {
    val localSpark = spark
    import localSpark.implicits._

    val profile = Seq((BigDecimal(10).bigDecimal, BigDecimal(2).bigDecimal)).toDF("LEFT_DIGITS", "RIGHT_DIGITS")
    val converter = new SchemaConverter(localSpark, "OWNER", "TABLE", "skip", new StubDbReader(profile), None)

    val field = converter.convertColumnInfoToStructField(ColumnInfo("NEG_SCALE", "NUMBER", "5", "-2"))

    assert(field.dataType == DecimalType(7, 0))
  }

  test("profiles NUMBER without precision in oracle mode") {
    val localSpark = spark
    import localSpark.implicits._

    val profile = Seq((BigDecimal(10).bigDecimal, BigDecimal(2).bigDecimal)).toDF("LEFT_DIGITS", "RIGHT_DIGITS")
    val converter = new SchemaConverter(localSpark, "OWNER", "TABLE", "oracle", new StubDbReader(profile), Some(99L))

    val field = converter.convertColumnInfoToStructField(ColumnInfo("PROFILED_NUM", "NUMBER", null, null))

    assert(field.dataType == DecimalType(12, 2))
  }

  test("castToSchema reorders and casts columns") {
    val localSpark = spark
    import localSpark.implicits._

    val profile = Seq((BigDecimal(10).bigDecimal, BigDecimal(2).bigDecimal)).toDF("LEFT_DIGITS", "RIGHT_DIGITS")
    val converter = new SchemaConverter(localSpark, "OWNER", "TABLE", "skip", new StubDbReader(profile), None)
    val source = Seq(("1", "2.50")).toDF("ID", "AMOUNT")
    val targetSchema = StructType(Seq(
      StructField("AMOUNT", DecimalType(10, 2), nullable = true),
      StructField("ID", IntegerType, nullable = true)
    ))

    val converted = converter.castToSchema(source, targetSchema)

    assert(converted.schema == targetSchema)
    val row = converted.first()
    assert(row.getDecimal(0) == new java.math.BigDecimal("2.50"))
    assert(row.getInt(1) == 1)
  }

  test("throws on unsupported Oracle types") {
    val localSpark = spark
    import localSpark.implicits._

    val profile = Seq((BigDecimal(10).bigDecimal, BigDecimal(2).bigDecimal)).toDF("LEFT_DIGITS", "RIGHT_DIGITS")
    val converter = new SchemaConverter(localSpark, "OWNER", "TABLE", "skip", new StubDbReader(profile), None)

    intercept[UnsupportedOperationException] {
      converter.convertColumnInfoToStructField(ColumnInfo("DOC", "XMLTYPE", null, null))
    }
  }
}
