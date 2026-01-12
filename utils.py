import asyncio
import json
import logging
from typing import Any

import orjson
import redis
import yaml

from apientreprises import BASE_DIR, logger

lock = asyncio.Lock()


async def load_settings_file() -> dict[str, Any]:
    with open(BASE_DIR / 'settings.yaml', 'r', encoding='utf-8') as f:
        settings = yaml.safe_load(f)
        logger.info('🟢 Loaded settings file successfully.')
        return settings


async def read_from_json(filename: str) -> dict:
    path = BASE_DIR / f'{filename}.json'
    if not path.exists():
        return {}

    with open(path, 'rb') as f:
        return orjson.loads(f.read())


async def write_to_json(data: dict, filename: str):
    if lock.locked():
        logging.warning(f'🟠 Waiting to write to JSON file ({filename})...')

    async with lock:
        path = BASE_DIR / f'{filename}.json'
        with open(path, mode='w') as f:
            try:
                clean_data = orjson.dumps(data, option=orjson.OPT_INDENT_2)
                items = json.loads(json.loads(clean_data))
                json.dump(items, f)
                logger.info(f'🟢 Successfully wrote to {filename}.json')
                return True
            except OSError:
                json.dump({}, f)
                raise
            except Exception as e:
                json.dump({}, f)
                logger.error(f'❌ Error writing to {filename}.json: {e}')
                raise


async def redis_connection() -> redis.asyncio.Redis:
    conn = redis.asyncio.from_url('redis://localhost', decode_responses=True)

    try:
        await conn.ping()
    except redis.ConnectionError as e:
        raise SystemExit(f'Error connecting to Redis: {e}')

    logger.info('🟢 Connected to Redis successfully.')
    return conn
