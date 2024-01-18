import asyncio
import aioredis
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def redis_listener(redis_url, channel_name, store):
    # Connect to Redis using aioredis 2.0+ syntax
    redis = aioredis.from_url(redis_url)
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel_name)

    try:
        async for message in pubsub.listen():
            if message['type'] == 'message':
                data = message['data'].decode('utf-8')
                store[channel_name] = data
                logging.info(f"Received new data on channel '{channel_name}': {data}")
    except asyncio.CancelledError:
        logging.info("Redis listener task was cancelled.")
    finally:
        await pubsub.unsubscribe(channel_name)
        await redis.close()

shared_store = {}
redis_url = "redis://localhost:6379"
channel_name = "eth:walletTracker:0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5_0xa27CEF8aF2B6575903b676e5644657FAe96F491F:wallettracking"

# Create and run the event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
listener_task = loop.create_task(redis_listener(redis_url, channel_name, shared_store))
try:
    loop.run_until_complete(listener_task)
except KeyboardInterrupt:
    listener_task.cancel()
    loop.run_until_complete(listener_task)
