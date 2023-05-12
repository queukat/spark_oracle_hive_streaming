package queukat.spark_universal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.math.ceil
// QueryGenerator is an object that provides methods for generating SQL queries to read data from a database.
// This method is used to generate an SQL query to read data from a database.
// The parameters represent the various parts that are used to form an SQL query.
//The object is responsible for generating SQL queries.

object QueryGenerator {
  //The method generates a request to get a table schema.
  def generateSchemaQuery(tableName: String, owner: String): String = {
    s"""SELECT t.COLUMN_NAME, t.DATA_TYPE, t.DATA_PRECISION, t.DATA_SCALE
       |FROM ALL_TAB_COLUMNS t
       |WHERE t.TABLE_NAME = '$tableName' AND t.OWNER = '$owner'""".stripMargin
  }

  //The method generates a universal query to get the metadata of the table.
  def generateUniversalQuery(owner: String, tableName: String): String = {
    s"""SELECT data_object_id, file_id, relative_fno, partition_name, subpartition_name,
       |MIN(start_block_id) AS start_block_id, MAX(end_block_id) AS end_block_id, SUM(blocks) AS blocks
       |FROM (SELECT o.data_object_id, p.partition_name, o.subobject_name as subpartition_name, e.file_id, e.relative_fno, e.block_id AS start_block_id,
       |e.block_id + e.blocks - 1 AS end_block_id, e.blocks
       |FROM dba_extents e, dba_objects o, dba_tab_partitions p, dba_tab_subpartitions tsp
       |WHERE o.owner = '$owner' AND o.object_name = '$tableName' AND e.owner = '$owner' AND e.segment_name = '$tableName'
       |AND o.owner = e.owner AND o.object_name = e.segment_name
       |AND (o.subobject_name = e.partition_name OR (o.subobject_name IS NULL AND e.partition_name IS NULL))
       |AND o.owner = p.table_owner(+) AND o.object_name = p.table_name(+) AND e.partition_name = p.partition_name(+)
       |AND o.owner = tsp.table_owner(+) AND o.object_name = tsp.table_name(+) AND o.subobject_name = tsp.subpartition_name(+))
       |GROUP BY data_object_id, file_id, relative_fno, partition_name, subpartition_name""".stripMargin
  }

  //The method generates a request to get data from the table.
  def generateDataQuery(queryColumns: String, owner: String, tableName: String, partitionInfo: DataFrame, numPartitions: Int): Seq[String] = {

    val totalBlocks = partitionInfo.collect().map(row => row.getAs[java.math.BigDecimal]("BLOCKS").longValue()).sum
    val blocksPerPartition = ceil(totalBlocks.toDouble / numPartitions).toLong

    partitionInfo.collect().flatMap(row => {
      val dataObjectId = row.getAs[Int]("data_object_id")
      val relativeFno = row.getAs[Int]("relative_fno")
      val startBlockId = row.getAs[Int]("start_block_id")
      val endBlockId = row.getAs[Int]("end_block_id")
      val partitionName = row.getAs[String]("partition_name")
      val subpartitionName = row.getAs[String]("subpartition_name")
      val blocks = row.getAs[Int]("blocks")

      val numPartitionsForCurrent = ceil(blocks.toDouble / blocksPerPartition).toInt

      (0 until numPartitionsForCurrent).map { partIndex =>
        val blockStart = startBlockId + (partIndex * blocksPerPartition).toInt
        val blockEnd = Seq(startBlockId + ((partIndex + 1) * blocksPerPartition).toInt - 1, endBlockId).min
        val baseQuery =
          s"""SELECT /*+ NO_INDEX($owner.$tableName) */ $queryColumns FROM $owner.$tableName WHERE
             |((rowid >= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $blockStart, 0)
             |AND rowid <= dbms_rowid.rowid_create(1, $dataObjectId, $relativeFno, $blockEnd, 32767)))""".stripMargin
        val partitionClause = if (partitionName != null) s" AND PARTITION_NAME = '$partitionName'" else ""
        val subpartitionClause = if (subpartitionName != null) s" AND SUBPARTITION_NAME = '$subpartitionName'" else ""
        baseQuery + partitionClause + subpartitionClause
      }
    })
  }
}
