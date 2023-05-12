package queukat.spark_universal

// // Fbreader is a class that is used to read data from a database.
// It uses JDBC to connect to a database and read data.

// This method is used to load data from a database using a set of SQL queries.
// The "queries" parameter is a set of SQL queries to execute.
// The method returns a set of dataframes, each of which contains data loaded using the corresponding SQL query.

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
//The class is responsible for reading data from the database using JDBC.
class DbReader(spark: SparkSession, url: String, oracleUser: String, oraclePassword: String, numPartitions: Int, fetchSize: Int) {
  //A method for reading data from a database using JDBC.
  def readFromJDBC(query: String): DataFrame = {
    val jdbcOptions = Map(
      "url" -> url,
      "user" -> oracleUser,
      "password" -> oraclePassword,
      "dbtable" -> s"(${query})",
      "sessionInitStatement" -> """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""",
      "numPartitions" -> numPartitions.toString,
      "fetchSize" -> fetchSize.toString,
      "driver" -> "oracle.jdbc.OracleDriver"
    )
    spark.read.format("jdbc").options(jdbcOptions).load()
  }

  def logQuery(query: String): Unit = {
    println(s"Executing query: $query")
  }

  def loadData(queryGroups: Seq[Seq[String]]): Future[DataFrame] = {
    val dataFutures: Seq[Future[Seq[DataFrame]]] = queryGroups.map { queryGroup =>
      Future.sequence(queryGroup.toSeq.map { query =>
        Future {
          logQuery(query)
          readFromJDBC(query)
        }
      })
    }
    // Start all future operations and return Future to wait for their execution.
    Future.sequence(dataFutures).map(_.flatten.reduce(_ union _))
  }
}
