package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Singleton object QueryGenerator.
 *
 * QueryGenerator provides generation of various types of SQL queries that are needed when interacting
 * with a table in a database. It can generate queries for retrieving table schema, table metadata or data from a table.
 */
object QueryGenerator {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def safeSqlString(str: String): String = {
    if (str == null || str.trim().isEmpty) {
      throw new IllegalArgumentException("Invalid table name or owner provided for SQL query.")
    }
    str.replaceAll("'", "''")
  }

  /**
   * Generates a SQL query to retrieve a table schema.
   *
   * This method is used when we need to fetch the schema of a specific table. The query generated by this method,
   * when executed, will return the column names, their data types, precision, and scale from the specified table.
   *
   * @param tableName the name of the table
   * @param owner     the owner of the table
   * @return a string representing the SQL query
   */
  def generateSchemaQuery(tableName: String, owner: String): String = {
    val safeTableName = safeSqlString(tableName)
    val safeOwner = safeSqlString(owner)
    s"""SELECT t.COLUMN_NAME, t.DATA_TYPE, t.DATA_PRECISION, t.DATA_SCALE
       |FROM ALL_TAB_COLUMNS t
       |WHERE t.TABLE_NAME = upper('$safeTableName') AND t.OWNER = upper('$safeOwner')""".stripMargin
  }
  /**
   * Generates a universal query to get the metadata of the table.
   *
   * This method generates a SQL query to fetch metadata about a table. The metadata includes details like
   * data_object_id, file_id, relative_fno, partition_name, subpartition_name, and block details.
   *
   * @param owner     the owner of the table
   * @param tableName the name of the table
   * @return a string representing the SQL query
   */
  def generateUniversalQuery(owner: String, tableName: String): String = {
    val safeTableName = safeSqlString(tableName)
    val safeOwner = safeSqlString(owner)


    s"""SELECT data_object_id, file_id, relative_fno, partition_name, subpartition_name,
       |MIN(start_block_id) AS start_block_id, MAX(end_block_id) AS end_block_id, SUM(blocks) AS blocks
       |FROM (SELECT o.data_object_id, p.partition_name, o.subobject_name as subpartition_name, e.file_id, e.relative_fno, e.block_id AS start_block_id,
       |e.block_id + e.blocks - 1 AS end_block_id, e.blocks
       |FROM dba_extents e, dba_objects o, dba_tab_partitions p, dba_tab_subpartitions tsp
       |WHERE o.owner = upper('$safeOwner') AND o.object_name = upper('$safeTableName') AND e.owner = upper('$safeOwner') AND e.segment_name = upper('$safeTableName')
       |AND o.owner = e.owner AND o.object_name = e.segment_name
       |AND (o.subobject_name = e.partition_name OR (o.subobject_name IS NULL AND e.partition_name IS NULL))
       |AND o.owner = p.table_owner(+) AND o.object_name = p.table_name(+) AND e.partition_name = p.partition_name(+)
       |AND o.owner = tsp.table_owner(+) AND o.object_name = tsp.table_name(+) AND o.subobject_name = tsp.subpartition_name(+))
       |GROUP BY data_object_id, file_id, relative_fno, partition_name, subpartition_name""".stripMargin
  }

  /**
   * Generates a query to retrieve data from the table.
   *
   * This method is used to fetch specific data from a table. It takes column names as input and constructs
   * a SQL query that fetches data for those columns. The generated queries also include the table's partition
   * details to enable efficient data retrieval.
   *
   * @param queryColumns  the columns to select in the query
   * @param owner         the owner of the table
   * @param tableName     the name of the table
   * @param partitionInfo dataframe containing partition information
   * @return a sequence of strings representing the SQL queries
   */
  def generateDataQuery(queryColumns: String, owner: String, tableName: String, partitionInfo: DataFrame): Seq[String] = {
    val safeQueryColumns = safeSqlString(queryColumns)
    val safeOwner = safeSqlString(owner)
    val safeTableName = safeSqlString(tableName)
    try {
      partitionInfo.collect().map(row => {
        val dataObjectId = row.getAs[java.math.BigDecimal]("DATA_OBJECT_ID").toBigInteger.longValue()
        val relativeFno = row.getAs[java.math.BigDecimal]("RELATIVE_FNO").toBigInteger.longValue()
        val startBlockId = row.getAs[java.math.BigDecimal]("START_BLOCK_ID").toBigInteger.longValue()
        val endBlockId = row.getAs[java.math.BigDecimal]("END_BLOCK_ID").toBigInteger.longValue()
        s"""SELECT /*+ NO_INDEX($safeOwner.$safeTableName) */ $safeQueryColumns FROM $owner.$tableName WHERE
           |(rowid >= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $startBlockId, 0)
           |AND rowid <= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $endBlockId, 32767))""".stripMargin
      })
    } catch {
      case e: Exception => logger.error(s"Failed to generate data query: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
}
