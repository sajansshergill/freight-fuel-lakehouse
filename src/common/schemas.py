from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType
)

load_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("load_id", StringType(), False),
    StructField("carrier_id", StringType(), True),
    StructField("lane_id", StringType(), True),
    StructField("status", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("expected_delivery_ts", TimestampType(), True),
    StructField("actual_delivery_ts", TimestampType(), True),
])

gps_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("truck_id", StringType(), False),
    StructField("driver_id", StringType(), True),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("speed_mph", DoubleType(), True),
    StructField("event_ts", TimestampType(), False),
])

fuel_txn_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("card_id", StringType(), True),
    StructField("truck_id", StringType(), False),
    StructField("driver_id", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("gallons", DoubleType(), False),
    StructField("price_per_gallon", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("event_ts", TimestampType(), False),
])