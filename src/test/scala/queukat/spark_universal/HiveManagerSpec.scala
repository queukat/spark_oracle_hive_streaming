package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import java.nio.file.Files

class HiveManagerSpec extends AnyFunSuite {

  test("saveAsTemporaryTable respects numPartitions") {
    // clear any existing session
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val warehouseDir = Files.createTempDirectory("hiveWarehouse").toFile.getAbsolutePath
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("HiveManagerSpec")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .enableHiveSupport()
      .getOrCreate()

    try {
      import spark.implicits._
      val df = Seq(1,2,3,4,5).toDF("num")
      val manager = new HiveManager(spark)
      val tempTable = "temp_numbers"
      manager.saveAsTemporaryTable(df, tempTable, numPartitions = 3)
      val partitions = spark.table(tempTable).rdd.getNumPartitions
      assert(partitions == 3)
      manager.dropTemporaryTable(tempTable)
    } finally {
      spark.stop()
    }
  }
}
