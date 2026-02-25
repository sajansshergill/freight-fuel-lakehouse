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

    # start around US-ish coordinates
    coords = {t: (40.7 + random.random(), -74.0 + random.random()) for t in trucks}

    print(f"[gps producer] bootstrap={settings.kafka_bootstrap} topic={settings.topic_gps}")
    while True:
        truck = random.choice(trucks)
        lat, lon = coords[truck]
        lat += random.uniform(-0.01, 0.01)
        lon += random.uniform(-0.01, 0.01)
        coords[truck] = (lat, lon)

        payload = {
            "event_id": str(uuid.uuid4()),
            "truck_id": truck,
            "driver_id": random.choice(drivers),
            "lat": lat,
            "lon": lon,
            "speed_mph": max(0.0, random.gauss(55, 10)),
            "event_ts": utcnow().isoformat(),
        }

        p.produce(settings.topic_gps, value=json.dumps(payload).encode("utf-8"))
        p.poll(0)
        time.sleep(0.1)

if __name__ == "__main__":
    main()