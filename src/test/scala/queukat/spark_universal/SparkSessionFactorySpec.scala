package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class SparkSessionFactorySpec extends AnyFunSuite {

  test("getSparkSession uses provided master") {
    // ensure previous sessions do not interfere
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val spark = SparkSessionFactory.getSparkSession("testApp", "local[1]")
    try {
      assert(spark.sparkContext.master == "local[1]")
    } finally {
      spark.stop()
    }
  }
}
