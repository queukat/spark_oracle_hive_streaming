package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class SparkSessionFactorySpec extends AnyFunSuite {

  test("getSparkSession uses provided master") {
    // ensure previous sessions do not interfere
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val originalMaster = System.getProperty("spark.master")
    System.clearProperty("spark.master")

    val spark = SparkSessionFactory.getSparkSession("testApp", "local[*]")
    try {
      assert(spark.sparkContext.master == "local[*]")
    } finally {
      spark.stop()
      if (originalMaster != null) System.setProperty("spark.master", originalMaster)
    }
  }
}
