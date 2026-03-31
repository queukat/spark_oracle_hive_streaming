package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class SparkSessionFactorySpec extends AnyFunSuite {
  private def stopIfRunning(session: SparkSession): Unit = {
    if (!session.sparkContext.isStopped) {
      session.stop()
    }
  }

  private def resetSparkSessions(): Unit = {
    SparkSession.getActiveSession.foreach(stopIfRunning)
    SparkSession.getDefaultSession.foreach(stopIfRunning)
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("getSparkSession uses provided master") {
    resetSparkSessions()

    val originalMaster = System.getProperty("spark.master")
    System.clearProperty("spark.master")

    val spark = SparkSessionFactory.getSparkSession("testApp", "local[*]")
    try {
      assert(spark.sparkContext.master == "local[*]")
    } finally {
      stopIfRunning(spark)
      resetSparkSessions()
      if (originalMaster != null) System.setProperty("spark.master", originalMaster)
    }
  }

  test("getSparkSession falls back to spark.master system property") {
    resetSparkSessions()

    val originalMaster = System.getProperty("spark.master")
    System.setProperty("spark.master", "local[1]")

    val spark = SparkSessionFactory.getSparkSession("testApp")
    try {
      assert(spark.sparkContext.master == "local[1]")
    } finally {
      stopIfRunning(spark)
      resetSparkSessions()
      if (originalMaster != null) System.setProperty("spark.master", originalMaster) else System.clearProperty("spark.master")
    }
  }
}
