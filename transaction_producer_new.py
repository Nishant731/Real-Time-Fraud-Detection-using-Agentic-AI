import requests
import json
from kafka import KafkaProducer
import time

API_URL = "http://data.630591349.xyz:8011/stream"
KAFKA_TOPIC = "fraud-transactions"

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers="127.0.0.1:9093",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5
            )
            print("Connected to Kafka.")
            return producer
        except Exception as e:
            print("Kafka connection failed, retrying...", e)
            time.sleep(3)

def stream_transactions():
    producer = create_producer()

    print(f"Connecting to API stream: {API_URL}")
    session = requests.Session()

    while True:
        try:
            with session.get(API_URL, stream=True, timeout=10) as response:
                print("Connected. Waiting for transactions...")

                for line in response.iter_lines():
                    if not line:
                        continue

                    try:
                        decoded = line.decode("utf-8")
                        data = json.loads(decoded)

                        # Send to Kafka
                        producer.send(KAFKA_TOPIC, data)
                        producer.flush()

                        print(f"Sent → {data}")

                    except json.JSONDecodeError:
                        print("Received malformed JSON, skipping:", line)
                    except Exception as e:
                        print("Error sending to Kafka:", e)

        except Exception as e:
            print("Stream connection lost. Reconnecting...", e)
            time.sleep(2)

if __name__ == "__main__":
    stream_transactions()

