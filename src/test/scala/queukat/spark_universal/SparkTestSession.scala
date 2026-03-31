package queukat.spark_universal

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.file.Files

trait SparkTestSession extends BeforeAndAfterAll { self: Suite =>
  private lazy val warehouseDir = Files.createTempDirectory("spark-universal-warehouse").toAbsolutePath.toString

  private var sparkSession: Option[SparkSession] = None

  private def stopIfRunning(session: SparkSession): Unit = {
    if (!session.sparkContext.isStopped) {
      session.stop()
    }
  }

  private def resetGlobalSparkSessions(): Unit = {
    SparkSession.getActiveSession.foreach(stopIfRunning)
    SparkSession.getDefaultSession.foreach(stopIfRunning)
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  protected def spark: SparkSession = synchronized {
    sparkSession.filterNot(_.sparkContext.isStopped).getOrElse {
      resetGlobalSparkSessions()

      val created = SparkSession.builder()
        .master("local[2]")
        .appName(self.getClass.getSimpleName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouseDir)
        .enableHiveSupport()
        .getOrCreate()

      sparkSession = Some(created)
      created
    }
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.foreach(stopIfRunning)
      sparkSession = None
      resetGlobalSparkSessions()
    } finally {
      super.afterAll()
    }
  }
}
