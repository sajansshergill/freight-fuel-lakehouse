import json, time, uuid, random
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.common.config import settings

def utcnow():
    return datetime.now(timezone.utc)

def main():
    p = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    trucks = [f"TR-{i:04d}" for i in range(1, 80)]
    drivers = [f"DR-{i:04d}" for i in range(1, 120)]
    stations = [f"ST-{i:05d}" for i in range(1, 300)]

    print(f"[fuel producer] bootstrap={settings.kafka_bootstrap} topic={settings.topic_fuel}")
    while True:
        gallons = max(5.0, random.gauss(40, 12))
        price = round(random.uniform(3.0, 4.5), 2)

        # occasional anomaly
        if random.random() < 0.02:
            gallons = random.uniform(80, 140)

        payload = {
            "event_id": str(uuid.uuid4()),
            "card_id": f"FC-{random.randint(10000, 99999)}",
            "truck_id": random.choice(trucks),
            "driver_id": random.choice(drivers),
            "station_id": random.choice(stations),
            "gallons": round(gallons, 2),
            "price_per_gallon": price,
            "total_amount": round(gallons * price, 2),
            "event_ts": utcnow().isoformat(),
        }

        p.produce(settings.topic_fuel, value=json.dumps(payload).encode("utf-8"))
        p.poll(0)
        time.sleep(0.7)

if __name__ == "__main__":
    main()