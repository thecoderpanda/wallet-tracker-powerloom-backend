import logging
import os
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
import redis.asyncio as aioredis
import requests
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import httpx
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

import tracemalloc
logging.basicConfig(level=logging.INFO)
tracemalloc.start()
import asyncio

## ENVIRONMENT VARIABLES

REDIS_URL = os.getenv("REDIS_HOST", "localhost")
MONGODB = os.getenv("MONGODB", "mongodb://localhost:27017")
IPFS_URL = os.getenv("IPFS_URL", "http://127.0.0.1:5001/api/v0")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "test@test.com")
SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD", "test")
SENDER_EMAIL_USERNAME = os.getenv("SENDER_EMAIL_USERNAME", "test")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = os.getenv("SMTP_PORT", 587)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Transaction(BaseModel):
    contract_interacted_with: str
    txhash: str
    timestamp: str

class User(BaseModel):
    wallet_address: str
    email: str

#MongoDB Connection
client = AsyncIOMotorClient(MONGODB)
db = client.userdb
users_collection = db.user  # Collection for users
transaction_store = db.transactions


# Global variable for the async Redis client
async_redis_client: aioredis.Redis = None

async def connect_to_async_redis():
    try:
        return await aioredis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        logging.error(f"Failed to connect to Async Redis: {e}")
        raise

async def fetch_ipfs_data(ipfs_hash: str):
    ipfs_api_url = f"{IPFS_URL}/cat?arg={ipfs_hash}"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(ipfs_api_url)
            response.raise_for_status()
            return response.content.decode()
        except httpx.RequestError as e:
            logging.error(f"Failed to fetch data from IPFS for hash {ipfs_hash}: {e}")
            return None

#SMTP
def send_email(receiver_email, subject, body):
    sender_email = SENDER_EMAIL
    password = SENDER_EMAIL_PASSWORD
    username = SENDER_EMAIL_USERNAME

    # Create a multipart message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    # Initialize the SMTP server
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()

    # Login to the server
    server.login(username, password)

    # Send the email
    server.sendmail(sender_email, receiver_email, message.as_string())

    # Quit the server
    server.quit()


async def check_wallet_and_fetch_ipfs_data(wallet_address, redis_key):
    try:
        formatted_wallet_address = wallet_address.strip()
        logging.info(f"Querying MongoDB for wallet address: {formatted_wallet_address}")

        user = users_collection.find_one({"wallet_address": formatted_wallet_address})

        if not user:
            logging.error(f"No user found in MongoDB for wallet: {formatted_wallet_address}")
            return

        #logging.info(f"User found: {user}")

        # user_email = user.get('email')
        # if not user_email:
        #     logging.error(f"No email found for user with wallet: {formatted_wallet_address}")
        #     return

        # Asynchronous call to Redis
        data = await async_redis_client.zrange(redis_key, 0, -1)
        if not data:
            logging.error(f"No data found in Redis for key: {redis_key}")
            return

        # Processing the IPFS data asynchronously
        for ipfs_hash in data:
            ipfs_data = await fetch_ipfs_data(ipfs_hash)
            if not ipfs_data:
                logging.error(f"No IPFS data found for hash {ipfs_hash}")
                continue

            try:
                ipfs_json = json.loads(ipfs_data)
                contract_address = ipfs_json.get("contract_address")
                transaction_hash = ipfs_json.get("transactionHash")

                transaction_document = {
                    "wallet_address": formatted_wallet_address,
                    "contract_address": contract_address,
                    "transaction_hash": transaction_hash
                }
                await transaction_store.insert_one(transaction_document)
                logging.info("Transaction data inserted into MongoDB")

                user_email = "shantanu@thecoderpanda.com"
                email_subject = "New Transaction Alert"
                email_body = (f"Hi,\n\nPowerloom detected a new transaction on your wallet: {formatted_wallet_address}\n"
                              f"Contract Address: {contract_address}\nTransaction Hash: {transaction_hash}\n\nThank you")
                send_email(user_email, email_subject, email_body)  
                logging.info(f"Email sent to {user_email}")

            except json.JSONDecodeError:
                logging.error(f"Failed to parse IPFS data for hash {ipfs_hash}")

    except Exception as e:
        logging.error(f"Error in check_wallet_and_fetch_ipfs_data: {e}")




async def key_event_listener():
    global async_redis_client
    if async_redis_client is None:
        logging.error("Redis client is not initialized.")
        return

    pubsub = async_redis_client.pubsub()
    await pubsub.psubscribe('__keyspace@0__:projectID:eth:walletTracker:*:wallettracking:finalizedData')
    async for message in pubsub.listen():
        if message['type'] == 'pmessage':
            full_key = message['channel'].split(':', 1)[-1]
            parts = full_key.split(':')
            if len(parts) > 3:
                wallet_address_part = parts[3]
                wallet_address = wallet_address_part.split('_')[0]
                logging.info(f"Key event received. Full key: {full_key}, Extracted wallet address: {wallet_address}")
                await check_wallet_and_fetch_ipfs_data(wallet_address, full_key)



@app.on_event("startup")
async def startup_event():
    global async_redis_client
    async_redis_client = await connect_to_async_redis()
    asyncio.create_task(key_event_listener())
    
@app.on_event("shutdown")
async def shutdown_event():
    global async_redis_client
    if async_redis_client:
        await async_redis_client.close()

@app.get("/get_wallet_data/{project_id}/{contract_id}")
async def get_wallet_data(project_id: str, contract_id: str):
    redis_key = f"projectID:eth:walletTracker:{project_id}_{contract_id}:wallettracking:finalizedData"
    if connect_to_async_redis.exists(redis_key):
        data = connect_to_async_redis.zrange(redis_key, 0, -1)
        decoded_data = [item.decode() for item in data]

        ipfs_data_list = []
        for ipfs_hash in decoded_data:
            ipfs_api_url = f"http://127.0.0.1:5001/api/v0/cat?arg={ipfs_hash}"
            try:
                response = requests.post(ipfs_api_url)
                response.raise_for_status()
                ipfs_data = response.content.decode()  # or response.text if the data is text
                ipfs_data_list.append(ipfs_data)
            except requests.RequestException as e:
                logging.error(f"Failed to fetch data from IPFS for hash {ipfs_hash}: {e}")
                continue

        return {"ipfs_data": ipfs_data_list}
    else:
        raise HTTPException(status_code=404, detail="Data not found")

# FastAPI endpoints
@app.post("/subscribe/")
async def subscribe(user: User):
    existing_user = await users_collection.find_one({"wallet_address": user.wallet_address})
    if existing_user:
        return {"status": "error", "message": "User already exists"}

    # Insert new user
    await users_collection.insert_one(user.dict())
    return {"status": "success", "message": "User subscribed successfully"}


