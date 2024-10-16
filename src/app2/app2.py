# app2.py
import os
import logging
from kafka import KafkaConsumer
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # You can change this to DEBUG for more detailed logs
    format='%(asctime)s %(levelname)s [%(otelTraceID)s:%(otelSpanID)s] - %(message)s'
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
APP3_SERVICE_URL = os.getenv('APP3_SERVICE_URL', 'http://localhost:8003')

consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group'
)

logging.info("App2 is listening to Kafka messages...")

for message in consumer:
    msg = message.value.decode('utf-8')
    logging.info(f"Received message: {msg}")
    if msg == 'it is good':
        # Send request to app3's 'good' endpoint
        try:
            response = requests.get(f'{APP3_SERVICE_URL}/good')
            response.raise_for_status()
            logging.info(f"Response from app3 (good): {response.json()}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending request to app3 (good): {e}")
    elif msg == 'it is bad':
        try:
            raise Exception('Received bad message')
        except Exception as e:
            # Send request to app3's 'bad' endpoint with error info
            try:
                response = requests.post(f'{APP3_SERVICE_URL}/bad', json={'error': str(e)})
                response.raise_for_status()
                logging.info(f"Response from app3 (bad): {response.json()}")
            except requests.exceptions.RequestException as ex:
                logging.error(f"Error sending request to app3 (bad): {ex}")
    else:
        logging.warning(f"Unhandled message: {msg}")