from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import threading
import logging

import pika
#from email.message import EmailMessage
import redis

logging.basicConfig(level=logging.INFO)


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# Connect to Redis
try:
    r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
except redis.RedisError:
    print("Failed to connect to Redis, check your Redis server and connection details.")

rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
rabbitmq_channel = rabbitmq_connection.channel()
queue_name = "hello"
#print("testing this")
#queue_name = "powerloom-processhub-commands-q:wallettracking:0xeF85a395631566291AAe51a37930304dB2f5E501"

# Function to process RabbitMQ messages

# Function to send email

# Set up RabbitMQ consumer in a separate thread
def rabbitmq_callback(ch, method, properties, body):
    print(f"Received message from RabbitMQ: {body}")
    logging.info(f"Received message from RabbitMQ: {body}")

def start_rabbitmq_listener():
    rabbitmq_channel.queue_declare(queue=queue_name, durable=False)
    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=rabbitmq_callback, auto_ack=True)
    rabbitmq_channel.start_consuming()

@app.on_event("startup")
async def startup_event():
    # Start the RabbitMQ consumer in a background thread
    thread = threading.Thread(target=start_rabbitmq_listener)
    thread.start()


class Transaction(BaseModel):
    contract_interacted_with: str
    txhash: str
    timestamp: str

class User(BaseModel):
    wallet_address: str
    email: str

@app.post("/subscribe/") 
def subscribe(user: User):
    key = f"user:{user.wallet_address}"

    if r.exists(key):
        return {"status": "error", "message": "User already exists"}

    # Store the user data
    r.hmset(key, user.dict())
    return {"status": "success", "message": "User subscribed successfully"}

@app.post("/transactions/")
def create_transaction(transaction: Transaction):
    # Tx hash is being treacted as unique key:
    key = f"transaction:{transaction.txhash}"
    if not r.exists(key):
        r.hmset(key, transaction.dict())  # Redis Storing of the data
        return {"status": "success", "transaction": transaction}
    else:
        raise HTTPException(status_code=400, detail="Transaction already exists")

@app.get("/transactions/{txhash}")
def read_transaction(txhash: str):
    key = f"transaction:{txhash}"
    transaction = r.hgetall(key)
    if transaction:
        return transaction
    raise HTTPException(status_code=404, detail="Transaction not found")

