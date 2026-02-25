import os
from pyspark.sql import functions as F
from src.common.config import settings
from src.common.spark import build_spark


def main():
    os.makedirs(settings.delta_base_path, exist_ok=True)
    spark = build_spark("build_gold_tables")

    load = spark.read.format("delta").load(f"{settings.delta_base_path}/silver_load_events")
    gps = spark.read.format("delta").load(f"{settings.delta_base_path}/silver_gps_events")
    fuel = spark.read.format("delta").load(f"{settings.delta_base_path}/silver_fuel_txn_events")

    # fact_load_status: last known status per load per day
    fact_load_status = (
        load.withColumn("event_date", F.to_date("event_ts"))
        .groupBy("load_id", "event_date")
        .agg(
            F.max_by("status", "event_ts").alias("latest_status"),
            F.max("event_ts").alias("latest_event_ts"),
            F.max("expected_delivery_ts").alias("expected_delivery_ts"),
            F.max("actual_delivery_ts").alias("actual_delivery_ts"),
            F.max("carrier_id").alias("carrier_id"),
            F.max("lane_id").alias("lane_id"),
        )
    )

    # fact_fuel_txn: keep events with anomaly flag
    fact_fuel_txn = (
        fuel.withColumn("event_date", F.to_date("event_ts"))
        .withColumn(
            "anomaly_flag",
            (
                (F.col("gallons") > 80)
                | (F.round(F.col("gallons") * F.col("price_per_gallon"), 2) != F.col("total_amount"))
            ).cast("int"),
        )
    )

    # fact_driver_movement: movement / mileage stats from GPS pings
    fact_driver_movement = (
        gps.withColumn("event_date", F.to_date("event_ts"))
        .groupBy("truck_id", "driver_id", "event_date")
        .agg(
            F.count("*").alias("ping_count"),
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.max("event_ts").alias("last_ping_ts"),
            F.min("event_ts").alias("first_ping_ts"),
        )
        .withColumn(
            "hours_observed",
            (F.col("last_ping_ts").cast("long") - F.col("first_ping_ts").cast("long")) / 3600,
        )
        .withColumn("est_miles", F.col("avg_speed_mph") * F.col("hours_observed"))
    )

    fact_load_status.write.format("delta").mode("overwrite").save(
        f"{settings.delta_base_path}/gold_fact_load_status"
    )
    fact_fuel_txn.write.format("delta").mode("overwrite").save(
        f"{settings.delta_base_path}/gold_fact_fuel_txn"
    )
    fact_driver_movement.write.format("delta").mode("overwrite").save(
        f"{settings.delta_base_path}/gold_fact_driver_movement"
    )

    print("[build_gold_tables] completed")


if __name__ == "__main__":
    main()