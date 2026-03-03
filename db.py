import os
import json
import logging
from datetime import datetime
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
REDIS_URI = os.environ.get("REDIS_URI", "redis://localhost:6379")
DB_NAME = os.environ.get("DB_NAME", "voidxmusicbot")

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]

songs_col = db["song_history"]
stats_col = db["user_stats"]
groups_col = db["group_settings"]
sponsor_col = db["sponsors"]

redis_client = None

async def init_db():
    global redis_client
    redis_client = await aioredis.from_url(REDIS_URI, decode_responses=True)
    await songs_col.create_index([("chat_id", 1), ("played_at", -1)])
    await stats_col.create_index([("user_id", 1)], unique=True)
    await groups_col.create_index([("chat_id", 1)], unique=True)
    await sponsor_col.create_index([("chat_id", 1)])
    logger.info("✅ Database connected!")

class RedisQueue:
    @staticmethod
    def _key(chat_id): return f"queue:{chat_id}"
    @staticmethod
    async def push(chat_id, track):
        await redis_client.rpush(RedisQueue._key(chat_id), json.dumps(track))
    @staticmethod
    async def pop(chat_id):
        val = await redis_client.lpop(RedisQueue._key(chat_id))
        return json.loads(val) if val else None
    @staticmethod
    async def peek(chat_id):
        val = await redis_client.lindex(RedisQueue._key(chat_id), 0)
        return json.loads(val) if val else None
    @staticmethod
    async def get_all(chat_id):
        items = await redis_client.lrange(RedisQueue._key(chat_id), 0, -1)
        return [json.loads(i) for i in items]
    @staticmethod
    async def length(chat_id):
        return await redis_client.llen(RedisQueue._key(chat_id))
    @staticmethod
    async def clear(chat_id):
        await redis_client.delete(RedisQueue._key(chat_id))
    @staticmethod
    async def shuffle(chat_id):
        import random
        items = await RedisQueue.get_all(chat_id)
        if len(items) < 2: return
        playing = items.pop(0)
        random.shuffle(items)
        items.insert(0, playing)
        await redis_client.delete(RedisQueue._key(chat_id))
        for item in items:
            await redis_client.rpush(RedisQueue._key(chat_id), json.dumps(item))
    @staticmethod
    async def rotate_loop(chat_id):
        val = await redis_client.lpop(RedisQueue._key(chat_id))
        if val:
            await redis_client.rpush(RedisQueue._key(chat_id), val)
            return json.loads(val)
        return None

class RedisState:
    @staticmethod
    def _key(chat_id, field): return f"state:{chat_id}:{field}"
    @staticmethod
    async def set(chat_id, field, value):
        await redis_client.set(RedisState._key(chat_id, field), json.dumps(value))
    @staticmethod
    async def get(chat_id, field, default=None):
        val = await redis_client.get(RedisState._key(chat_id, field))
        return json.loads(val) if val is not None else default
    @staticmethod
    async def toggle(chat_id, field):
        current = await RedisState.get(chat_id, field, False)
        new_val = not current
        await RedisState.set(chat_id, field, new_val)
        return new_val

async def log_play(chat_id, user_id, username, track):
    now = datetime.utcnow()
    await songs_col.insert_one({
        "chat_id": chat_id, "user_id": user_id,
        "username": username, "title": track["title"],
        "url": track.get("webpage_url", ""),
        "duration": track.get("duration", 0), "played_at": now,
    })
    await stats_col.update_one(
        {"user_id": user_id},
        {
            "$inc": {"total_plays": 1, "total_duration": track.get("duration", 0)},
            "$set": {"username": username, "last_seen": now},
            "$setOnInsert": {"joined": now},
        },
        upsert=True
    )
    count = await redis_client.incr(f"play_count:{chat_id}")
    return count

async def get_top_songs(chat_id=None, limit=10):
    match = {"chat_id": chat_id} if chat_id else {}
    pipeline = [
        {"$match": match},
        {"$group": {"_id": "$title", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]
    return await songs_col.aggregate(pipeline).to_list(length=limit)

async def get_user_stats(user_id):
    return await stats_col.find_one({"user_id": user_id}, {"_id": 0})

async def get_group_history(chat_id, limit=10):
    cursor = songs_col.find(
        {"chat_id": chat_id},
        {"_id": 0, "title": 1, "username": 1, "played_at": 1}
    ).sort("played_at", -1).limit(limit)
    return await cursor.to_list(length=limit)

async def get_top_users(chat_id=None, limit=10):
    match = {}
    if chat_id:
        user_ids = await songs_col.distinct("user_id", {"chat_id": chat_id})
        match = {"user_id": {"$in": user_ids}}
    cursor = stats_col.find(match, {"_id": 0}).sort("total_plays", -1).limit(limit)
    return await cursor.to_list(length=limit)

async def get_group_settings(chat_id):
    doc = await groups_col.find_one({"chat_id": chat_id})
    if not doc:
        default = {
            "chat_id": chat_id, "volume": 100,
            "auto_leave": True, "idle_timeout": 300,
            "thumbnail": True, "mode_247": False,
            "created_at": datetime.utcnow(),
        }
        await groups_col.insert_one(default)
        return default
    return doc

async def update_group_setting(chat_id, key, value):
    await groups_col.update_one(
        {"chat_id": chat_id},
        {"$set": {key: value}},
        upsert=True
    )

async def add_sponsor(chat_id, name, message, interval=10, added_by=0):
    await sponsor_col.insert_one({
        "chat_id": chat_id, "name": name,
        "message": message, "interval": interval,
        "active": True, "added_by": added_by,
        "plays_shown": 0, "created_at": datetime.utcnow(),
    })

async def get_active_sponsors(chat_id):
    cursor = sponsor_col.find({"chat_id": chat_id, "active": True})
    return await cursor.to_list(length=20)

async def remove_sponsor(chat_id, name):
    result = await sponsor_col.delete_one({"chat_id": chat_id, "name": name})
    return result.deleted_count > 0

async def list_sponsors(chat_id):
    cursor = sponsor_col.find({"chat_id": chat_id}, {"_id": 0})
    return await cursor.to_list(length=20)

async def increment_sponsor_plays(chat_id, name):
    await sponsor_col.update_one(
        {"chat_id": chat_id, "name": name},
        {"$inc": {"plays_shown": 1}}
    )

async def check_and_send_sponsor(chat_id, play_count, send_func):
    sponsors = await get_active_sponsors(chat_id)
    for sponsor in sponsors:
        if play_count % sponsor.get("interval", 10) == 0:
            await send_func(sponsor["message"])
            await increment_sponsor_plays(chat_id, sponsor["name"])
    return True
