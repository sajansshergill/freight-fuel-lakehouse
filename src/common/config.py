import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    
    delta_base_path: str = os.getenv("DELTA_BASE_PATH", "./data/delta")
    checkpoint_base_path: str = os.getenv("CHECKPOINT_BASE_PATH", "./data/checkpoints")
    
    topic_load: str = os.getenv("TOPIC_LOAD_EVENTS", "load_events")
    topic_gps: str = os.getenv("TOPIC_GPS_EVENTS", "gps_events")
    topic_fuel: str = os.getenv("TOPIC_FUEL_TXN_EVENTS", "fuel_txn_events")
    
settings = Settings()