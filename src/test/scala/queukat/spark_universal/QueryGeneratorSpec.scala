package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class QueryGeneratorSpec extends AnyFunSuite {

  test("generateSchemaQuery escapes input") {
    val query = QueryGenerator.generateSchemaQuery("t'ab", "ow'ner")
    assert(query.contains("upper('t''ab')"))
    assert(query.contains("upper('ow''ner')"))
  }

  test("generateDataQuery returns queries") {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._
    val df = Seq((1,1,1,1)).toDF("DATA_OBJECT_ID", "RELATIVE_FNO", "START_BLOCK_ID", "END_BLOCK_ID")
    val queries = QueryGenerator.generateDataQuery("COL1", "OWNER", "TABLE", df)
    assert(queries.nonEmpty)
    spark.stop()
  }
}
