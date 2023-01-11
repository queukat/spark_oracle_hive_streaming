# spark_oracle_migration_streaming
# OracleToHive

This project is a Scala application that uses Apache Spark to transfer data from an Oracle database to a Hive table in ORC format. It also uses Spark Streaming to capture data changes in the Oracle table and append them to the Hive table.

## Prerequisites

- Apache Spark
- Oracle Database
- Hive

## Installation

1. Clone the repository
2. Compile the Scala code
3. Run the application with the following arguments:
    - Oracle Table Name
    - Hive Table Name
    - Oracle URL
    - Hive URL
    - Oracle User
    - Oracle Password
    - Hive User
    - Hive Password

## Usage

The application will transfer the data from the Oracle table to the Hive table in ORC format. It will also use Spark Streaming to capture data changes in the Oracle table and append them to the Hive table.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
