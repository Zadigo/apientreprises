import asyncio
import json
import logging
import pathlib

import orjson
from pydantic import ValidationError
import redis
import yaml

from apientreprises import BASE_DIR, logger
from apientreprises.models import SettingsModel

lock = asyncio.Lock()


async def load_settings_file() -> SettingsModel:
    with open(BASE_DIR / 'settings.yaml', 'r', encoding='utf-8') as f:
        settings = yaml.safe_load(f)
        logger.info('🟢 Loaded settings file successfully.')

        try:
            settings_model = SettingsModel(**settings)
        except ValidationError as e:
            raise SystemExit(f'Error in settings file: {e}')
        return settings_model


async def read_from_json(filename: str) -> dict:
    path = BASE_DIR / f'{filename}.json'
    if not path.exists():
        return {}

    with open(path, 'rb') as f:
        return orjson.loads(f.read())


async def write_to_json(data: dict, filename: str | pathlib.Path) -> bool:
    if lock.locked():
        logging.warning(f'🟠 Waiting to write to JSON file ({filename})...')

    async with lock:
        if isinstance(filename, str):
            path = BASE_DIR / f'{filename}.json'
        else:
            path = filename

            if not path.suffix == '.json':
                path = path.with_suffix('.json')

        with open(path, mode='w') as f:
            try:
                clean_data = orjson.dumps(data, option=orjson.OPT_INDENT_2)

                items = json.loads(clean_data)
                if isinstance(items, str):
                    items = json.loads(items)

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
