import os
from pyspark.sql import functions as F
from src.common.config import settings
from src.common.spark import build_spark


def stream_topic_to_bronze(spark, topic: str, out_path: str, ckpt: str):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    
    parsed = (
        df.select(
            F.col("topic").cast("string").alias("topic"),
            F.col("partition").alias("partition"),
            F.col("offset").alias("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw_data"),
            F.col("value").cast("string").alias("value_json"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("event_date", F.to_date("kafka_ts"))
    )
    
    return (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", ckpt)
        .partitionBy("event_date")
        .outputMode("append")
        .start(out_path)
    )


def main():
    os.makedirs(settings.delta_base_path, exist_ok=True)
    os.makedirs(settings.checkpoint_base_path, exist_ok=True)
    
    spark = build_spark("bronze_stream")

    q1 = stream_topic_to_bronze(
        spark,
        settings.topic_load,
        f"{settings.delta_base_path}/bronze_load_events",
        f"{settings.checkpoint_base_path}/bronze_load_events",
    )
    q2 = stream_topic_to_bronze(
        spark,
        settings.topic_gps,
        f"{settings.delta_base_path}/bronze_gps_events",
        f"{settings.checkpoint_base_path}/bronze_gps_events",
    )
    q3 = stream_topic_to_bronze(
        spark,
        settings.topic_fuel,
        f"{settings.delta_base_path}/bronze_fuel_txn_events",
        f"{settings.checkpoint_base_path}/bronze_fuel_txn_events",
    )

    print("[bronze_stream] running... Ctrl+C to stop")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()