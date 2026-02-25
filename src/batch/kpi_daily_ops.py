import os
from pyspark.sql import functions as F
from src.common.config import settings
from src.common.spark import build_spark

def main():
    os.makedirs(settings.delta_base_path, exist_ok=True)
    spark = build_spark("kpi_daily_ops")

    loads = spark.read.format("delta").load(f"{settings.delta_base_path}/gold_fact_load_status")
    fuel = spark.read.format("delta").load(f"{settings.delta_base_path}/gold_fact_fuel_txn")
    move = spark.read.format("delta").load(f"{settings.delta_base_path}/gold_fact_driver_movement")

    delivered = loads.filter(F.col("latest_status") == "delivered")

    kpi_on_time = (
        delivered.withColumn(
            "on_time",
            (F.col("actual_delivery_ts").isNotNull() & (F.col("actual_delivery_ts") <= F.col("expected_delivery_ts"))).cast("int")
        )
        .groupBy("event_date")
        .agg(
            F.countDistinct("load_id").alias("delivered_loads"),
            F.avg("on_time").alias("on_time_rate"),
        )
    )

    kpi_fuel = (
        fuel.groupBy("event_date")
            .agg(
                F.sum("total_amount").alias("fuel_spend"),
                F.sum("gallons").alias("fuel_gallons"),
                F.sum("anomaly_flag").alias("fuel_anomaly_txns"),
            )
    )

    kpi_miles = (
        move.groupBy("event_date")
            .agg(
                F.sum("est_miles").alias("est_miles"),
                F.avg("avg_speed_mph").alias("avg_speed_mph"),
            )
    )

    kpi_daily_ops = (
        kpi_on_time.join(kpi_fuel, "event_date", "outer")
                  .join(kpi_miles, "event_date", "outer")
                  .withColumn("fuel_spend_per_mile", F.col("fuel_spend") / F.when(F.col("est_miles") > 0, F.col("est_miles")).otherwise(None))
                  .orderBy(F.col("event_date").desc())
    )

    kpi_daily_ops.write.format("delta").mode("overwrite").save(f"{settings.delta_base_path}/gold_kpi_daily_ops")
    print("[kpi_daily_ops] wrote gold_kpi_daily_ops")

if __name__ == "__main__":
    main()