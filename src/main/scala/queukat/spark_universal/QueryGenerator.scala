package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object QueryGenerator {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def safeSqlLiteral(str: String): String = {
    if (str == null || str.trim.isEmpty) {
      throw new IllegalArgumentException("Invalid table name, owner, or column provided for SQL query.")
    }
    str.replace("'", "''")
  }

  def quoteIdentifier(identifier: String): String = {
    if (identifier == null || identifier.trim.isEmpty) {
      throw new IllegalArgumentException("Invalid identifier provided for SQL query.")
    }
    "\"" + identifier.replace("\"", "\"\"") + "\""
  }

  def qualifyTable(owner: String, tableName: String): String = {
    s"${quoteIdentifier(owner)}.${quoteIdentifier(tableName)}"
  }

  def renderSelectColumns(columns: Seq[String]): String = {
    if (columns.isEmpty) {
      throw new IllegalArgumentException("At least one column is required to generate a data query.")
    }
    columns.map(quoteIdentifier).mkString(", ")
  }

  def generateSchemaQuery(tableName: String, owner: String): String = {
    val safeTableName = safeSqlLiteral(tableName)
    val safeOwner = safeSqlLiteral(owner)
    s"""SELECT t.COLUMN_NAME, t.DATA_TYPE, t.DATA_PRECISION, t.DATA_SCALE
       |FROM ALL_TAB_COLUMNS t
       |WHERE t.TABLE_NAME = upper('$safeTableName') AND t.OWNER = upper('$safeOwner')
       |ORDER BY t.COLUMN_ID""".stripMargin
  }

  def generateUniversalQuery(owner: String, tableName: String): String = {
    val safeTableName = safeSqlLiteral(tableName)
    val safeOwner = safeSqlLiteral(owner)

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

  def generateDataQuery(
    queryColumns: Seq[String],
    owner: String,
    tableName: String,
    partitionInfo: DataFrame,
    snapshotScn: Option[Long] = None
  ): Seq[String] = {
    val selectColumns = renderSelectColumns(queryColumns)
    val qualifiedTableName = qualifyTable(owner, tableName)
    val flashbackClause = snapshotScn.map(scn => s" AS OF SCN $scn").getOrElse("")

    try {
      partitionInfo.collect().map { row =>
        val dataObjectId = row.getAs[java.math.BigDecimal]("DATA_OBJECT_ID").toBigInteger.longValue()
        val relativeFno = row.getAs[java.math.BigDecimal]("RELATIVE_FNO").toBigInteger.longValue()
        val startBlockId = row.getAs[java.math.BigDecimal]("START_BLOCK_ID").toBigInteger.longValue()
        val endBlockId = row.getAs[java.math.BigDecimal]("END_BLOCK_ID").toBigInteger.longValue()
        s"""SELECT $selectColumns
           |FROM $qualifiedTableName$flashbackClause
           |WHERE rowid >= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $startBlockId, 0)
           |AND rowid <= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $endBlockId, 32767)""".stripMargin
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to generate data query: ${e.getMessage}", e)
        throw e
    }
  }

  def generateNumberProfileQuery(
    owner: String,
    tableName: String,
    columnName: String,
    snapshotScn: Option[Long] = None
  ): String = {
    val qualifiedTableName = qualifyTable(owner, tableName)
    val qualifiedColumn = quoteIdentifier(columnName)
    val flashbackClause = snapshotScn.map(scn => s" AS OF SCN $scn").getOrElse("")
    val numericExpr = s"TO_CHAR(ABS($qualifiedColumn), 'TM9', 'NLS_NUMERIC_CHARACTERS=.,')"

    s"""SELECT
       |MAX(CASE
       |  WHEN $qualifiedColumn IS NULL THEN 1
       |  WHEN INSTR($numericExpr, '.') > 0 THEN INSTR($numericExpr, '.') - 1
       |  ELSE LENGTH($numericExpr)
       |END) AS LEFT_DIGITS,
       |MAX(CASE
       |  WHEN $qualifiedColumn IS NULL THEN 0
       |  WHEN INSTR($numericExpr, '.') > 0 THEN LENGTH(RTRIM(SUBSTR($numericExpr, INSTR($numericExpr, '.') + 1), '0'))
       |  ELSE 0
       |END) AS RIGHT_DIGITS
       |FROM $qualifiedTableName$flashbackClause
       |WHERE $qualifiedColumn IS NOT NULL""".stripMargin
  }
}
