# Project: Spark Universal Migrator

The Spark Universal Migrator is a data migration Spark application that fetches data from an Oracle database and stores it in a Hive database. It utilizes JDBC to connect to Oracle, processes and converts the data into a Hive-compatible schema, and then transfers it to Hive. The migration process employs iterators to generate queries for data retrieval and utilizes a BlockingQueue in conjunction with a ThreadPoolExecutor for parallel processing of data. Additionally, the application incorporates an Oracle connection pool to efficiently manage and reuse database connections, further optimizing performance and resource utilization during the migration process.

# Requirements

Apache Spark 3.4
Scala 2.12.x or later
An Oracle database to serve as the source
A Hive database to serve as the target
JDBC driver for Oracle

# Setup

Clone the repository to your local machine.
Ensure that Apache Spark 3.4 and Scala are properly installed on your machine.
Ensure your Oracle and Hive databases are accessible and the JDBC driver for Oracle is correctly set up.

# Running the Application

To run the application, execute the migrate function from NewSpark object. This function requires the following parameters:

- url: The JDBC URL for your Oracle database
- oracleUser: The username for your Oracle database
- oraclePassword: The password for your Oracle database
- tableName: The name of the table you wish to migrate from Oracle
- owner: The owner of the table in Oracle
- hivetable: The name of the Hive table where data will be inserted
- numPartitions: The number of partitions to be used when reading from Oracle
- fetchSize: The fetch size to use when reading from Oracle
- typeCheck: A strategy to handle Oracle NUMBER data types. If set to "spark," Spark will decide the data type; otherwise, the Oracle data type will be used.

Please replace these parameters with your actual values.

# Example

```scala
import queukat.spark_universal.NewSpark

object ExampleMigration {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:oracle:thin:@//localhost:1521/ORCL" // JDBC URL for Oracle
    val oracleUser = "your_oracle_username" // Oracle database user
    val oraclePassword = "your_oracle_password" // Oracle database password
    val tableName = "employees" // Name of the Oracle table to migrate
    val owner = "HR" // Owner of the Oracle table
    val hivetable = "employees_hive" // Name of the Hive table where the data will be written
    val numPartitions = 10 // Number of partitions to use when reading the Oracle data into a DataFrame
    val fetchSize = 1000 // Number of rows to fetch at a time from Oracle
    val typeCheck = "spark" // Strategy to handle Oracle NUMBER data types (spark or oracle)

    try {
      NewSpark.migrate(url, oracleUser, oraclePassword, tableName, owner, hivetable, numPartitions, fetchSize, typeCheck)
      println("Data migration from Oracle to Hive completed successfully.")
    } catch {
      case e: Exception => println(s"Data migration failed. Reason: ${e.getMessage}")
    }
  }
}

```

<details>
  <summary><strong>Logic of convertColumnInfoToStructField method (click to expand)</strong></summary>

The convertColumnInfoToStructField method performs the conversion of a ColumnInfo object into a StructField, which represents a column in the Spark schema. This conversion takes into account the differences between Oracle and Spark data types and handles special cases related to numeric values that may be large for Oracle but not supported by Spark.


Parameters:

info: A ColumnInfo object representing column information in the Oracle schema.
Conversion Logic:

1. The method first determines the Spark data type corresponding to the Oracle data type based on the value of the dataType property in the ColumnInfo object.
2. It then creates a Metadata object that will be used for additional settings of the StructField, if necessary.
3. Depending on the dataType, the method performs the following actions:
   - For "VARCHAR2" data type: Sets the data type to StringType with empty metadata.
   - For "DATE" data type: Sets the data type to TimestampType with empty metadata.
   - For "NUMBER" data type:
     - If typeCheck is "skip": Calculates the precision and scale of the number and sets the data type to DecimalType with these values and empty metadata.
     - If typeCheck is "oracle": If the precision and scale of the number are not defined (null), the method executes additional queries to the Oracle database to determine the maximum number of digits to the left and right of the decimal point. If the sum of these values is greater than 38, it sets the data type to StringType; otherwise, it sets the data type to DecimalType with the determined precision and scale and empty metadata.
     - If typeCheck is "spark": If the precision and scale of the number are not defined (null), the method adds a special key "toAnalyze" to the metadata with the value "true", indicating that the schema should be analyzed and converted later. Otherwise, it sets the data type to DecimalType with the determined precision and scale and empty metadata.
     - For any other value of typeCheck, an exception is thrown as the value is invalid.
   - For "FLOAT" data type: Sets the data type to FloatType with empty metadata.
   - For "DOUBLE" data type: Sets the data type to DoubleType with empty metadata.
   - For "INT" data type: Sets the data type to IntegerType with empty metadata.
   - For "BOOLEAN" data type: Sets the data type to BooleanType with empty metadata.
   
The method handles various cases to ensure the correct conversion of data types from Oracle to Spark.

</details>

# Known Limitations
This application currently supports only Oracle data types that can be converted to Hive data types. If a column in Oracle has a data type that can't be converted to a Hive data type, the application will skip this column.

# Contributing
If you'd like to contribute, please fork the repository and make changes as you'd like. Pull requests are warmly welcome.
