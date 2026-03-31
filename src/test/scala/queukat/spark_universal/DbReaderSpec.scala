package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class DbReaderSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  private class StubDbReader(results: Map[String, DataFrame]) extends DbReader(
    spark,
    "jdbc:oracle:thin:@//localhost:1521/ORCL",
    "user",
    "password",
    numPartitions = 2,
    fetchSize = 1000
  ) {
    override def readFromJDBC(query: String): DataFrame = {
      results.getOrElse(query, throw new IllegalArgumentException(s"Unexpected query in test: $query"))
    }
  }

  test("captureSnapshotScn reads SCN through JDBC") {
    val reader = new StubDbReader(
      Map("SELECT dbms_flashback.get_system_change_number AS CURRENT_SCN FROM dual" -> Seq(BigDecimal(42).bigDecimal).toDF("CURRENT_SCN"))
    )

    assert(reader.captureSnapshotScn() == 42L)
  }

  test("loadData unions Spark JDBC result DataFrames") {
    val query1 = "select * from source_part_1"
    val query2 = "select * from source_part_2"
    val reader = new StubDbReader(
      Map(
        query1 -> Seq((1, "A")).toDF("ID", "VALUE"),
        query2 -> Seq((2, "B")).toDF("ID", "VALUE")
      )
    )

    val result = reader.loadData(
      Iterator(query1, query2),
      StructType(Seq(StructField("ID", IntegerType), StructField("VALUE", StringType)))
    )

    val rows = result.orderBy("ID").collect()
    assert(rows.length == 2)
    assert(rows(0).getInt(0) == 1)
    assert(rows(1).getInt(0) == 2)
  }

  test("loadData returns empty DataFrame with fallback schema when no queries were generated") {
    val reader = new StubDbReader(Map.empty)
    val fallbackSchema = StructType(Seq(StructField("ID", IntegerType), StructField("VALUE", StringType)))

    val result = reader.loadData(Iterator.empty, fallbackSchema)

    assert(result.schema == fallbackSchema)
    assert(result.count() == 0)
  }
}
