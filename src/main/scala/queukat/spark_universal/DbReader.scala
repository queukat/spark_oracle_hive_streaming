package queukat.spark_universal

import oracle.ucp.jdbc.{PoolDataSource, PoolDataSourceFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import queukat.spark_universal.ResourceCleanup.queue

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{BlockingQueue, Callable, Executors, FutureTask, LinkedBlockingQueue, Semaphore, ThreadPoolExecutor}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Try

/**
 * Class DbReader.
 *
 * The DbReader class provides functionality for reading data from a database using JDBC. It manages a connection pool to
 * allow concurrent processing of queries. The class provides methods for both simple and complex queries.
 *
 * Simple queries are executed directly and the result is returned as a DataFrame, which is suitable for smaller datasets
 * or optimized queries, like aggregations. Complex queries are executed using a connection pool, allowing
 * greater concurrency and therefore can handle larger datasets. The result of these queries is returned as an Iterator of Rows.
 *
 * The DbReader takes care of session initialization, query execution, and resource cleanup, like closing connections, statements, and result sets.
 *
 * @constructor Creates a new DbReader with a spark session, url, oracle user, oracle password, number of partitions, and fetch size.
 * @param spark          the SparkSession used for transformations
 * @param url            the url of the database
 * @param oracleUser     the username for the oracle database
 * @param oraclePassword the password for the oracle user
 * @param numPartitions  the number of partitions to use when reading data
 * @param fetchSize      the number of rows to fetch at once
 */
class DbReader(
                private val spark: SparkSession,
                private val url: String,
                private val oracleUser: String,
                private val oraclePassword: String,
                private val numPartitions: Int,
                private val fetchSize: Int
              ) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val sessionInitStatement = """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;"""
  val pendingQueries: BlockingQueue[String] = new LinkedBlockingQueue[String]()
  val threadPool: ThreadPoolExecutor = Executors.newFixedThreadPool(numPartitions).asInstanceOf[ThreadPoolExecutor]
  private val pds: PoolDataSource = initDataSource()

  private def initDataSource(): PoolDataSource = {
    val pds = PoolDataSourceFactory.getPoolDataSource()
    pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource")
    pds.setUser(oracleUser)
    pds.setPassword(oraclePassword)
    pds.setURL(url)
    pds.setInitialPoolSize(numPartitions)
    pds.setMaxPoolSize(numPartitions)
    pds
  }

  logger.info("DbReader created")

  /**
   * Method readFromJDBC.
   *
   * Executes a simple query on the database and returns the result as a DataFrame. This is done by passing
   * a map of JDBC options to Spark's DataFrameReader, which handles the execution of the query
   * and converts the result into a DataFrame.
   * This method is suitable for smaller datasets or queries that are optimized by the database, such as aggregates.
   *
   * @param query A valid SQL query to be executed against the Oracle database.
   * @return a DataFrame representing the retrieved data
   */
  def readFromJDBC(query: String): DataFrame = {
    logger.info(s"Starting readFromJDBC with query: $query")
    val jdbcOptions = Map(
      "url" -> url,
      "user" -> oracleUser,
      "password" -> oraclePassword,
      "dbtable" -> s"(${query})",
      "sessionInitStatement" -> sessionInitStatement,
      "numPartitions" -> numPartitions.toString,
      "fetchSize" -> fetchSize.toString,
      "driver" -> "oracle.jdbc.OracleDriver"
    )
    val df = spark.read.format("jdbc").options(jdbcOptions).load()
    logger.info("readFromJDBC completed successfully")
    df
  }

  private class QueryExecutionTask(query: String) extends Callable[Iterator[Row]] {
    override def call(): Iterator[Row] = {
      logger.info(s"Executing QueryExecutionTask")
      downloadFromJDBC(query)
    }
  }
  /**
   * Method loadData.
   *
   * Executes a series of complex queries using a connection pool to parallelize the execution. The result of each query
   * is transformed into an RDD of Rows. All these RDDs are then unionized to form a single RDD, which is then converted
   * into a DataFrame.
   * This method is suitable for large datasets that can be processed concurrently.
   *
   * @param queryIterator an Iterator of SQL queries to be executed
   * @param schema        The schema of the expected data. This should match the schema of the database or table the queries are targeting.
   * @return a Future of DataFrame representing the loaded data
   */
  def loadData(queryIterator: Iterator[String], schema: StructType)
              (implicit executionContext: ExecutionContext): Future[DataFrame] = {
    logger.info("Starting loadData method")
    val futures: List[Future[RDD[Row]]] = prepareFuturesFromQueries(queryIterator)
    composeDataFrameFromFutures(futures, schema)
  }

  private def prepareFuturesFromQueries(queryIterator: Iterator[String])
                                       (implicit executionContext: ExecutionContext): List[Future[RDD[Row]]] = {
    queryIterator.map(query => {
      val task = new QueryExecutionTask(query)
      val futureTask = new FutureTask(task)
      threadPool.submit(futureTask)
      futureTask
    }).map(futureTask => Future {
      executeTaskWithRecovery(futureTask)
    }(executionContext)).toList
  }

  private def executeTaskWithRecovery(task: FutureTask[Iterator[Row]]): RDD[Row] = {
    try {
      spark.sparkContext.parallelize(blocking(task.get()).toSeq)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load data for query. Reason: ${e.getMessage}", e)
        spark.sparkContext.emptyRDD[Row]
    }
  }

  private def composeDataFrameFromFutures(futures: List[Future[RDD[Row]]], schema: StructType)
                                         (implicit executionContext: ExecutionContext): Future[DataFrame] = {
    Future.sequence(futures).map { rdds =>
      val unionRdd = rdds.reduce(_ union _)
      spark.createDataFrame(unionRdd, schema)
    }
  }

  /**
   * Method downloadFromJDBC.
   *
   * Connects to the database, executes a query, and transforms the result into an Iterator of Rows. The connection, statement,
   * and result set are closed automatically once the result has been fully consumed. The closing of these resources is done
   * in a separate thread to prevent blocking the main thread.
   *
   * @param query the SQL query to be executed
   * @return an Iterator of Rows representing the result of the query
   */
  @tailrec
  private def downloadFromJDBC(query: String, attempt: Int = 0): ResultSetIterator = {
    logger.info(s"Starting downloadFromJDBC with query: $query")
    var conn: Connection = null
    try {
      conn = establishConnection(query)
      executeQuery(conn, query)
    } catch {
      case e: SQLException =>
        logger.error(s"Failed to execute query: $query, attempt: $attempt, reason: ${e.getMessage}", e)
        if(conn != null) Try(conn.close())
        if(attempt < 4) downloadFromJDBC(query, attempt + 1) else throw e
    }
  }
  private def establishConnection(query: String): Connection = {
    try {
      pds.getConnection()
    } catch {
      case _: SQLException =>
        logger.error("All connections in the pool are in use. Adding query to the queue...")
        pendingQueries.add(query)
        throw new SQLException("All connections are busy, the query has been added to the pending queue")
    }
  }

  private def executeQuery(conn: Connection, query: String): ResultSetIterator = {
    logger.info(s"Executing query: $query")
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    try {
      val initStmt = conn.prepareStatement(sessionInitStatement)
      initStmt.execute()
      stmt = conn.prepareStatement(query)
      stmt.setFetchSize(fetchSize)
      rs = stmt.executeQuery()
      new ResultSetIterator(rs, stmt, conn, queue)
    } catch {
      case e: Exception =>
        closeResources(rs, stmt, conn)
        throw e
    }
  }

  private def closeResources(rs: ResultSet, stmt: PreparedStatement, conn: Connection): Unit = {
    Try(if (rs != null) rs.close()).recover {
      case e: Exception => logger.error(s"Failed to close ResultSet. Reason: ${e.getMessage}", e)
    }
    Try(if (stmt != null) stmt.close()).recover {
      case e: Exception => logger.error(s"Failed to close PreparedStatement. Reason: ${e.getMessage}", e)
    }
    Try(conn.close()).recover {
      case e: Exception => logger.error(s"Failed to close Connection. Reason: ${e.getMessage}", e)
    }
  }

  @volatile private var running = true

  private def handlePendingQueries(): Unit = {
    try {
      while (running) {
        val query = pendingQueries.take()
        downloadFromJDBC(query)
      }
    } catch {
      case _: InterruptedException =>
        logger.info("Pending query handler interrupted")
    }
  }

  private val pendingThread: Thread = {
    val t = new Thread(() => handlePendingQueries())
    t.setDaemon(true)
    t.start()
    t
  }

  def close(): Unit = {
    running = false
    pendingThread.interrupt()
    threadPool.shutdown()
  }
}
