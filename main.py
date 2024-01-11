from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

app = FastAPI()

# Connect to Redis
try:
    r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
except redis.RedisError:
    print("Failed to connect to Redis, check your Redis server and connection details.")

class Transaction(BaseModel):
    user_email: str
    user_wallet: str
    contract_interacted_with: str
    txhash: str
    timestamp: str

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

