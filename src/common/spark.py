from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # Add Kafka connector via Delta's helper so it doesn't overwrite packages
    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()