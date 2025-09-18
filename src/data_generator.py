import time, json, uuid, random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pages = ['/home','/product','/product/1','/cart','/checkout']
events = ['view','click','add_to_cart','purchase']
countries = ['IN','US','GB','DE','SG']

def gen_event():
    return {
        "user_id": random.randint(1, 1000),
        "session_id": str(uuid.uuid4()),
        "page_url": random.choice(pages),
        "event_type": random.choice(events),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "device": random.choice(['mobile','desktop','tablet']),
        "country": random.choice(countries)
    }

if __name__ == "__main__":
    topic = "clickstream-events"
    print("Starting producer -> topic:", topic)
    try:
        while True:
            ev = gen_event()
            producer.send(topic, ev)
            print("sent", ev)
            time.sleep(0.05)
    except KeyboardInterrupt:
        print("stopped")
