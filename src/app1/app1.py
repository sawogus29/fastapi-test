import os
from fastapi import FastAPI
from kafka import KafkaProducer

app = FastAPI()

producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'))

@app.get("/good")
async def send_good():
    message = 'it is good'
    producer.send('my_topic', message.encode('utf-8'))
    producer.flush()
    return {"status": f"Message sent: {message}"}

@app.get("/bad")
async def send_bad():
    message = 'it is bad'
    producer.send('my_topic', message.encode('utf-8'))
    producer.flush()
    return {"status": f"Message sent: {message}"}