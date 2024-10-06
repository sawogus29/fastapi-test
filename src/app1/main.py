import os
import logging
from fastapi import FastAPI, Request
from kafka import KafkaProducer
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

# Initialize Kafka producer and thread pool executor
producer = None
executor = ThreadPoolExecutor(max_workers=10)

@app.on_event("startup")
def startup_event():
    """Initialize Kafka producer on startup."""
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode('utf-8')
    )
    logger.info("Kafka producer started")

@app.on_event("shutdown")
def shutdown_event():
    """Close Kafka producer on shutdown."""
    if producer:
        producer.close()
        logger.info("Kafka producer stopped")

# Middleware to log requests
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        old_factory = logging.getLogRecordFactory()
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.otelSpanID = "123456"
            return record
        logging.setLogRecordFactory(record_factory)
        logger.info(f"Incoming request: {request.method} {request.url}")
        logging.setLogRecordFactory(old_factory)
        response = await call_next(request)
        logger.info(f"Response status code: {response.status_code}")
        return response

# Add the middleware to the app
app.add_middleware(LoggingMiddleware)

# Function to send message to Kafka (runs in a thread)
def send_message_to_kafka(message):
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    logger.info(f"Sent message to Kafka: {message}")

# Endpoint to produce a message to Kafka
@app.get("/")
async def root():
    message = "Hello, World!"
    loop = asyncio.get_running_loop()
    # Use run_in_executor to avoid blocking the event loop
    await loop.run_in_executor(executor, send_message_to_kafka, message)
    return {"message": message}

# Health check endpoint
@app.get("/health")
async def health():
    logger.info(f"This should print valid otelTraceID & otelSpanID")
    return {"status": "ok"}
