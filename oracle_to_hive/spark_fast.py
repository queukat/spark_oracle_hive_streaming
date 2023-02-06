import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class ColumnInfo:
    def __init__(self, column_name, data_type, data_precision, data_scale):
        self.columnName = column_name
        self.dataType = data_type
        self.dataPrecision = data_precision
        self.dataScale = data_scale


class NewSpark:
    def migrate(self, url, oracle_user, oracle_password, table_name, owner, hive_table):
        spark = SparkSession.builder \
            .appName("OracleToHiveMigrator") \
            .enableHiveSupport() \
            .getOrCreate()

        jdbcOptions = {
            "url": url,
            "user": oracle_user,
            "password": oracle_password
        }

        oracle_schema = spark.read.format("jdbc").options(jdbcOptions) \
            .option("dbtable", f"(SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE "
                               f"from ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table_name}')") \
            .load()

        query_columns = oracle_schema.select("COLUMN_NAME")

        casted_schema = oracle_schema.select(F.col("COLUMN_NAME").alias("column_name"),
                                             F.col("DATA_TYPE").alias("data_type"),
                                             F.col("DATA_PRECISION").alias("data_precision"),
                                             F.col("DATA_SCALE").alias("data_scale"))

        StructType([
            StructField("column_name", StringType(), nullable=True),
            StructField("data_type", StringType(), nullable=True),
            StructField("data_precision", StringType(), nullable=True),
            StructField("data_scale", StringType(), nullable=True)
        ])

        casted_schema.rdd.map(
            lambda row: ColumnInfo(row.columnName, row.dataType, row.dataPrecision, row.dataScale)).map(
            lambda colInfo: StructField(colInfo.columnName,
                                        self.__getHiveDataType(colInfo, spark, url, oracle_user, oracle_password, owner,
                                                               table_name), nullable=True))

        create_table_sql = f"CREATE TABLE {hive_table} ( {', '.join([f'{row.columnName} {row.dataType}' for row in casted_schema.collect()])} )"
        spark.sql(create_table_sql)

        file_ids = spark.read.format("jdbc").options(jdbcOptions) \
            .option("dbtable", f"(SELECT data_object_id,file_id, relative_fno, subobject_name, MIN(start_block_id) "
                               f"start_block_id, MAX(end_block_id)   end_block_id, SUM(blocks)  blocks   "
                               f"FROM (SELECT o.data_object_id, o.subobject_name, e.file_id, e.relative_fno, "
                               f"e.block_id  start_block_id, e.block_id + e.blocks - 1 end_block_id, e.blocks   "
                               f"FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp   WHERE o.owner = "
                               f"{owner} AND o.object_name = {table_name} AND e.owner = {owner} AND e.segment_name = "
                               f"{table_name} AND o.owner = e.owner AND o.object_name = e.segment_name AND "
                               f"(o.subobject_name = e.partition_name   OR (o.subobject_name IS NULL   AND "
                               f"e.partition_name IS NULL)) AND o.owner = tsp.table_owner(+) "
                               f"AND o.object_name = tsp.table_name(+) AND o.subobject_name = tsp.subpartition_name(+)) "
                               f"GROUP BY data_object_id, file_id, relative_fno, subobject_name "
                               f"ORDER BY data_object_id, file_id, relative_fno, subobject_name;)") \
            .load()

        query_dfs = file_ids.select("relative_fno", "data_object_id", "start_block_id", "end_block_id") \
            .repartition(10).map(lambda row: spark.sql(
            f"SELECT /*+ NO_INDEX(t) */ {', '.join([str(col) for col in query_columns.collect()])} FROM "
            f"{owner}.{table_name} WHERE ((rowid >= dbms_rowid.rowid_create(1, {row.data_object_id}, "
            f"{row.relative_fno}, {row.start_block_id}, 0) AND rowid <= dbms_rowid.rowid_create(1, "
            f"{row.data_object_id}, {row.relative_fno}, {row.end_block_id}, 32767)))")) \
            .reduce(lambda df1, df2: df1.union(df2))

        query_dfs.write.mode("append").insertInto(hive_table)

    def __getHiveDataType(self, colInfo, spark, url, oracleUser, oraclePassword, owner, tableName):
        if colInfo.dataType == "VARCHAR2":
            return StringType()
        elif colInfo.dataType == "DATE":
            return TimestampType()
        elif colInfo.dataType == "NUMBER":
            if colInfo.data_precision is None or colInfo.dataScale is None:
                max_left_of_decimal = spark.read.format("jdbc") \
                    .options({"url": url, "dbtable": f"(select max(abs(trunc({colInfo.column_name},0))) from "
                                                     f"{owner}.{tableName})",
                              "user": oracleUser,
                              "password": oraclePassword}) \
                    .load() \
                    .first()[0] \
                    .length
                max_right_of_decimal = spark.read.format("jdbc") \
                    .options(
                    {"url": url, "dbtable": f"(select max(mod({colInfo.columnName}, 1)) from {owner}.{tableName})",
                     "user": oracleUser, "password": oraclePassword}).load().first()[0].toString().substring(3).length
                if max_left_of_decimal + max_right_of_decimal > 38:
                    return StringType()
                else:
                    return DecimalType(max_left_of_decimal, max_right_of_decimal)
            else:
                return DecimalType(colInfo.dataPrecision.toInt(), colInfo.dataScale.toInt())
