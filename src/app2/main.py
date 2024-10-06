import os
import logging
from fastapi import FastAPI, Request
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import asyncio
from starlette.middleware.base import BaseHTTPMiddleware

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'my_topic'

# Initialize thread pool executor
executor = ThreadPoolExecutor(max_workers=10)

# Middleware to log requests
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logger.info(f"Incoming request: {request.method} {request.url}", extra={'otelSpanID': '123456'})
        response = await call_next(request)
        logger.info(f"Response status code: {response.status_code}")
        return response

# Add the middleware to the app
app.add_middleware(LoggingMiddleware)

# Function to consume messages from Kafka (runs in a thread)
def consume_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='app2-group',
        value_deserializer=lambda v: v.decode('utf-8')
    )
    messages = []
    for msg in consumer:
        decoded_value = msg.value
        logger.info(f"Received message from Kafka: {decoded_value}")
        messages.append(decoded_value)
        if len(messages) >= 10:  # Adjust as needed
            break
    consumer.close()
    return messages

# Endpoint to consume messages from Kafka
@app.get("/consume")
async def consume():
    """Consume messages from Kafka topic."""
    loop = asyncio.get_running_loop()
    messages = await loop.run_in_executor(executor, consume_messages)
    return {"messages": messages}

# Health check endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}
