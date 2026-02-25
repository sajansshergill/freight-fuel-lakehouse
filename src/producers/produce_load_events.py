import json, time, uuid, random
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from src.common.config import settings

STATUSES = ["created", "assigned", "picked_up", "delivered", "cancelled"]

def utcnow():
    return datetime.now(timezone.utc)

def main():
    p = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    load_ids = [f"LD-{i:05d}" for i in range(1, 200)]
    carriers = [f"CR-{i:03d}" for i in range(1, 30)]
    lanes = [f"LN-{i:03d}" for i in range(1, 50)]

    print(f"[Load producer] bootstrap={settings.kafka_bootstrap} topic={settings.topic_load}")
    while True:
        load_id = random.choice(load_ids)
        event_id = str(uuid.uuid4())
        status = random.choices(STATUSES, weights=[40, 25, 15, 15, 5], k=1)[0]
        
        event_ts = utcnow()
        expected = event_ts + timedelta(hours=random.randint(6, 48))
        actual = None
        if status == "delivered":
            actual = expected + timedelta(hours=random.randint(-2, 8))
            
        payload = {
            "event_id": event_id,
            "load_id": load_id,
            "carrier_id": random.choice(carriers),
            "lane_id": random.choice(lanes),
            "status": status,
            "event_ts": event_ts.isoformat(),
            "expected_delivery_ts": expected.isoformat(),
            "actual_delivery_ts": actual.isoformat() if actual else None,
        }
        
        p.produce(settings.topic_load, value=json.dumps(payload).encode("utf-8"))
        p.poll(0)
        time.sleep(0.2)
        
if __name__ == "__main__":
    main()
