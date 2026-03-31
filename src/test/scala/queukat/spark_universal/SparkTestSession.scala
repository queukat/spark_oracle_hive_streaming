package queukat.spark_universal

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.file.Files

trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>
  private lazy val warehouseDir = Files.createTempDirectory("spark-universal-warehouse").toAbsolutePath.toString

  protected lazy val spark: SparkSession = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    SparkSession.builder()
      .master("local[2]")
      .appName(self.getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .enableHiveSupport()
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      if (SparkSession.getActiveSession.isDefined || SparkSession.getDefaultSession.isDefined) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }
}
