import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.config import settings
from src.common.schemas import load_events_schema, gps_events_schema, fuel_txn_events_schema
from src.common.spark import build_spark


def parse_and_clean(bronze_path: str, schema, entity: str):
    # read bronze delta as streaming
    df = (
        spark.readStream.format("delta")
        .load(bronze_path)
        .select("value_json", "kafka_ts")
    )
    
    typed = (
        df.select(F.from_json("value_json", schema).alias("j"), "kafka_ts")
        .select("j.*", "kafka_ts")
        .withColumn("event_ts", F.col("event_ts")) # alreadt timestamp by schema
    )
    
    # basic validity rules (expand as needed)
    valid = typed
    if entity == "fuel":
        valid = valid.filter((F.col("gallons") > 0) & (F.col("price_per_gallon") > 0) & (F.col("total_amount") > 0))
    if entity == "gps":
        valid = valid.filter((F.col("lat").between(-90, 90)) & (F.col("lon").between(-180, 180)))
        
    # watermark + dropDuplicates for streaming dedupe
    deduped = (
        valid.withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return deduped

def write_silver(df, out_path: str, ckpt: str):
    return (
        df.writeStream.format("delta")
        .option("checkpointLocation", ckpt)
        .partitionBy("event_date")
        .outputMode("append")
        .start(out_path)
    )
    
def main():
    os.makedirs(settings.delta_base_path, exist_ok=True)
    os.makedirs(settings.checkpoint_base_path, exist_ok=True)
    
    global spark
    spark = build_spark("silver_stream")
    
    load_df = parse_and_clean(
        f"{settings.delta_base_path}/bronze_load_events",
        load_events_schema,
        "load",
    )
    
    gps_df = parse_and_clean(
        f"{settings.delta_base_path}/bronze_gps_events",
        gps_events_schema,
        "gps",
    )
    
    fuel_df = parse_and_clean(
        f"{settings.delta_base_path}/bronze_fuel_txn_events",
        fuel_txn_events_schema,
        "fuel",
    )
    
    q1 = write_silver(load_df, f"{settings.delta_base_path}/silver_load_events", f"{settings.checkpoint_base_path}/silver_load_events")
    q2 = write_silver(gps_df, f"{settings.delta_base_path}/silver_gps_events", f"{settings.checkpoint_base_path}/silver_gps_events")
    q3 = write_silver(fuel_df, f"{settings.delta_base_path}/silver_fuel_txn_events", f"{settings.checkpoint_base_path}/silver_fuel_txn_events")
    
    print("[silver_stream] running... Ctrl+C to stop")
    spark.streams.awaitAnyTermination()
    
if __name__ == "__main__":
    main()