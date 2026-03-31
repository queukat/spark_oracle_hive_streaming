package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class HiveManagerSpec extends AnyFunSuite with SparkTestSession {
  private def randomTableName(prefix: String): String = {
    s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"
  }

  test("insertDataIntoHiveTable creates a target table and overwrites existing data on rerun") {
    import spark.implicits._

    val manager = new HiveManager(spark)
    val targetTable = randomTableName("target_table")
    val tempTable1 = randomTableName("temp_table")
    val tempTable2 = randomTableName("temp_table")

    try {
      manager.saveAsTemporaryTable(Seq((1, "first")).toDF("id", "value"), tempTable1, numPartitions = 1)
      manager.insertDataIntoHiveTable(tempTable1, targetTable, spark.table(tempTable1).schema)
      assert(spark.table(targetTable).count() == 1)

      manager.dropTemporaryTable(tempTable1)

      manager.saveAsTemporaryTable(Seq((2, "second")).toDF("id", "value"), tempTable2, numPartitions = 1)
      manager.insertDataIntoHiveTable(tempTable2, targetTable, spark.table(tempTable2).schema)

      val rows = spark.table(targetTable).collect()
      assert(rows.length == 1)
      assert(rows.head.getInt(0) == 2)
      assert(rows.head.getString(1) == "second")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS `$tempTable1`")
      spark.sql(s"DROP TABLE IF EXISTS `$tempTable2`")
      spark.sql(s"DROP TABLE IF EXISTS `$targetTable`")
    }
  }
}
