package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class SparkSessionFactorySpec extends AnyFunSuite {

  test("getSparkSession uses provided master") {
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

  test("getSparkSession falls back to spark.master system property") {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val originalMaster = System.getProperty("spark.master")
    System.setProperty("spark.master", "local[1]")

    val spark = SparkSessionFactory.getSparkSession("testApp")
    try {
      assert(spark.sparkContext.master == "local[1]")
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      if (originalMaster != null) System.setProperty("spark.master", originalMaster) else System.clearProperty("spark.master")
    }
  }
}
