# Project: Spark Universal Migrator
This is a Spark application designed to fetch data from an Oracle database and store it in a Hive database. The data is fetched via JDBC, processed and converted into a compatible schema for Hive, and then stored in Hive.

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

url: The JDBC URL for your Oracle database
oracleUser: The username for your Oracle database
oraclePassword: The password for your Oracle database
tableName: The name of the table you wish to migrate from Oracle
owner: The owner of the table in Oracle
hivetable: The name of the Hive table where data will be inserted
numPartitions: The number of partitions to be used when reading from Oracle
fetchSize: The fetch size to use when reading from Oracle
skipTypeCheck: A boolean to determine whether to skip checking Oracle data types for compatibility with Hive.
Please replace these parameters with your actual values.

# Known Limitations
This application currently supports only Oracle data types that can be converted to Hive data types. If a column in Oracle has a data type that can't be converted to a Hive data type, the application will skip this column.

# Contributing
If you'd like to contribute, please fork the repository and make changes as you'd like. Pull requests are warmly welcome.
