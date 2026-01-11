import asyncio
import uuid
from typing import Any

import httpx
import orjson
import orjson
import json
import redis
import yaml
from pydantic import ValidationError
from asgiref.sync import sync_to_async

from apientreprises import BASE_DIR, logger
from apientreprises.models import SettingsModel, UrlsModel

lock = asyncio.Lock()


async def load_settings_file() -> dict[str, Any]:
    with open(BASE_DIR / 'settings.yaml', 'r', encoding='utf-8') as f:
        settings = yaml.safe_load(f)
    return settings


async def read_from_json(filename: str) -> dict:
    path = BASE_DIR / f'{filename}.json'
    if not path.exists():
        return {}

    with open(path, 'rb') as f:
        return orjson.loads(f.read())


async def write_to_json(data: dict, filename: str):
    async with lock:
        path = BASE_DIR / f'{filename}.json'
        with open(path, mode='w') as f:
            try:
                clean_data = orjson.dumps(data, option=orjson.OPT_INDENT_2)
                items = json.loads(json.loads(clean_data))
                json.dump(items, f)
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

    return conn


async def requester(url: str, settings: SettingsModel):
    try:
        async with httpx.AsyncClient(timeout=settings.conf.wait_time) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.RequestError as e:
        logger.error(f'❌ Error fetching {url}: {e}')
        return None
    else:
        logger.info(f'✅ Url: {url} ({response.status_code})')
        return response.json()


async def celery_processor():
    while True:
        await asyncio.sleep(20)


async def processor(items: UrlsModel, settings: SettingsModel):
    while True:
        counter = 0
        async with asyncio.TaskGroup() as tg:
            while counter <= 10:
                url = items.pending_urls.pop(0)

                try:
                    async with asyncio.timeout(300):
                        task = tg.create_task(requester(url, settings))
                        task.add_done_callback(lambda t: None)
                        counter += 1
                except asyncio.TimeoutError:
                    logger.info(f'❌ Timeout while processing URL: {url}')
                except Exception as e:
                    logger.info(
                        f'❌ An exception occurred while processing tasks: {e}')

            if counter >= 10:
                counter = 0

        await asyncio.sleep(settings.conf.iteration_wait_time)


async def main():
    logger.info('🚀 Starting API Enterprises Processor')
    conn = await redis_connection()

    runid: str | uuid.UUID | None = None
    run_data = await read_from_json('run')
    previous_runid = run_data.get('runid', None)

    if previous_runid is not None:
        conn = await redis_connection()
        runid = await conn.get(previous_runid)

    if runid is None:
        runid = str(uuid.uuid4())
        await conn.hset(runid, 'counter', 0)

    # Load settings file
    settings = await load_settings_file()

    try:
        settings_model = SettingsModel(**settings)
    except ValidationError as e:
        raise SystemExit(f'Error in settings file: {e}')

    urls_file = await read_from_json('urls')

    try:
        urls_model = UrlsModel(**urls_file)
    except ValidationError as e:
        raise SystemExit(f'Error in URLs file: {e}')

    if not urls_model.pending_urls and not urls_model.done_urls:
        for i in range(settings_model.conf.pagination + 1):
            page_url = f'https://example.com/urls?page={i}'
            urls_model.pending_urls.append(page_url)

        data = urls_model.model_dump_json()
        await write_to_json(data, 'urls')
        await conn.hset(runid, 'urls', data)

    logger.info(f'🔑 Run ID: {runid}')
    logger.info(f'📥 Pending URLs: {len(urls_model.pending_urls)}')

    t1 = asyncio.create_task(processor(urls_model, settings_model))
    t2 = asyncio.create_task(celery_processor())
    await asyncio.gather(t1, t2)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(f'Error: {e}')
    except KeyboardInterrupt:
        logger.info('✅ Process interrupted by user.')
