package queukat.spark_universal

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import java.lang.ref.{PhantomReference, ReferenceQueue}
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.util.{Failure, Success, Try}

/**
 * An iterator over a JDBC [[ResultSet]] which provides methods to iterate through and fetch rows from the [[ResultSet]].
 * It uses a phantom reference to track when it can be automatically closed.
 *
 * @param rs    ResultSet obtained from the JDBC query
 * @param stmt  PreparedStatement used to create the ResultSet
 * @param conn  Connection object used to create the PreparedStatement
 * @param queue ReferenceQueue used for PhantomReference
 */
class ResultSetIterator(rs: ResultSet, stmt: PreparedStatement, conn: Connection, queue: ReferenceQueue[ResultSetIterator]) extends Iterator[Row] with AutoCloseable {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private var rowCounter = 0

  var hasNext: Boolean = {
    val next = rs.next()
    if (next && rowCounter < 10) logger.info("Obtained the next record from ResultSet")
    else if (!next) logger.info("No more records in ResultSet")
    next
  }

  private val phantomRef = new PhantomReference(this, queue)

  /**
   * Closes the [[ResultSet]], [[PreparedStatement]], and [[Connection]].
   * Called by finalize or when there are no more elements left to iterate.
   */
  def close(): Unit = {
    phantomRef.clear()
    closeResource("ResultSet", rs.close())
    closeResource("PreparedStatement", stmt.close())
    closeResource("Connection", conn.close())
  }

  private def closeResource(resourceName: String, closeAction: => Unit): Unit = {
    Try(closeAction) match {
      case Success(_) => logger.info(s"Successfully closed $resourceName")
      case Failure(e) => logger.error(s"Error closing $resourceName", e)
    }
  }

  /**
   * Fetches the next row from the ResultSet.
   *
   * @return A Row object containing the data of the next row in the [[ResultSet]].
   * @throws NoSuchElementException if there are no more rows in the [[ResultSet]] or if the [[ResultSet]] is closed.
   */
  def next(): Row = {
    if (!hasNext) {
      if (rs.isClosed) {
        throw new NoSuchElementException("ResultSet already closed.")
      } else {
        throw new NoSuchElementException("No more rows in ResultSet.")
      }
    }
    val metaData = rs.getMetaData
    val numColumns = metaData.getColumnCount
    val arr = new Array[Any](numColumns)
    for (i <- 0 until numColumns) {
      arr(i) = rs.getObject(i + 1)
    }
    hasNext = {
      val next = rs.next()
      if (next && rowCounter < 10) {
        rowCounter += 1
        logger.info("Obtained the next record from ResultSet")
      } else if (!next) logger.info("No more records in ResultSet")
      next
    }
    if (!hasNext) {
      logger.info("No more records available, closing resources")
      close()
    }
    Row.fromSeq(arr)  }
}
