package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite

class QueryGeneratorSpec extends AnyFunSuite with SparkTestSession {
  import spark.implicits._

  test("generateSchemaQuery escapes literal input") {
    val query = QueryGenerator.generateSchemaQuery("t'ab", "ow'ner")
    assert(query.contains("upper('t''ab')"))
    assert(query.contains("upper('ow''ner')"))
  }

  test("generateDataQuery quotes identifiers and includes snapshot SCN") {
    val partitionInfo = Seq((
      BigDecimal(1).bigDecimal,
      BigDecimal(2).bigDecimal,
      BigDecimal(10).bigDecimal,
      BigDecimal(20).bigDecimal
    )).toDF("DATA_OBJECT_ID", "RELATIVE_FNO", "START_BLOCK_ID", "END_BLOCK_ID")

    val query = QueryGenerator.generateDataQuery(
      Seq("COL 1", "VALUE"),
      "OWN\"ER",
      "TAB'LE",
      partitionInfo,
      Some(123L)
    ).head

    assert(query.contains("SELECT \"COL 1\", \"VALUE\""))
    assert(query.contains("FROM \"OWN\"\"ER\".\"TAB'LE\" AS OF SCN 123"))
    assert(query.contains("dbms_rowid.rowid_create(1, 1, 2, 10, 0)"))
    assert(query.contains("dbms_rowid.rowid_create(1, 1, 2, 20, 32767)"))
  }

  test("generateNumberProfileQuery includes flashback clause and quoted column") {
    val query = QueryGenerator.generateNumberProfileQuery("OWNER", "TABLE", "VALUE", Some(77L))
    assert(query.contains("FROM \"OWNER\".\"TABLE\" AS OF SCN 77"))
    assert(query.contains("WHERE \"VALUE\" IS NOT NULL"))
  }
}
