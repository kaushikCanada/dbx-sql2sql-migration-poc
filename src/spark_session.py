import os
from pyspark.sql import SparkSession

MSSQL_JDBC_PACKAGE  = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
DELTA_PACKAGE       = "io.delta:delta-spark_2.13:4.0.0"  # requires Spark 4.0.x


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("migration-poc")
        .config("spark.jars.packages", f"{MSSQL_JDBC_PACKAGE},{DELTA_PACKAGE}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def jdbc_url(server: str, port: str, database: str) -> str:
    return f"jdbc:sqlserver://{server}:{port};databaseName={database};encrypt=true;trustServerCertificate=true"


def jdbc_props(user: str, password: str) -> dict:
    return {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def source_jdbc_url() -> str:
    return jdbc_url(
        os.environ["SOURCE_SERVER"],
        os.environ.get("SOURCE_PORT", "1433"),
        os.environ["SOURCE_DB"],
    )


def target_jdbc_url() -> str:
    return jdbc_url(
        os.environ["TARGET_SERVER"],
        os.environ.get("TARGET_PORT", "1433"),
        os.environ["TARGET_DB"],
    )


def source_props() -> dict:
    return jdbc_props(os.environ["SOURCE_USER"], os.environ["SOURCE_PASSWORD"])


def target_props() -> dict:
    return jdbc_props(os.environ["TARGET_USER"], os.environ["TARGET_PASSWORD"])
