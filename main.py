import asyncio
from typing import Any
import uuid

import httpx
from pydantic import ValidationError
from apientreprises import tasks as celery_tasks
from apientreprises import logger
from apientreprises.models import SettingsModel, UrlsModel
from apientreprises.utils import (load_settings_file,
                                  read_from_json, redis_connection, write_to_json)

data_queue = asyncio.Queue()


async def requester(url: str, settings: SettingsModel) -> tuple[int | None, dict[str, Any] | None]:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.RequestError as e:
        logger.error(f'❌ Error fetching {url}: {e}')
        return None, None
    except Exception as e:
        logger.error(f'❌ Unexpected error for {url}: {e}')
        return None, None
    else:
        logger.info(f'✅ Url: {url} ({response.status_code})')
        return response.status_code, response.json()


async def celery_processor(debug_mode: bool = False):
    while True:
        if not data_queue.empty():
            data = await data_queue.get()

            # Process the data (e.g., save to database, further analysis, etc.)
            logger.info(f'⚪️ Processing data: {data}')
            celery_tasks.clean_data.apply_async(args=[data])

        if debug_mode:
            break

        await asyncio.sleep(20)


async def processor(items: UrlsModel, settings: SettingsModel, debug_mode: bool = False):
    conn = await redis_connection()

    while True:
        counter = 0

        async with asyncio.TaskGroup() as tg:
            while counter <= 10:
                try:
                    url = items.pending_urls.pop(0)
                except IndexError:
                    logger.info('✅ No more pending URLs to process.')
                    break

                try:
                    async with asyncio.timeout(300):
                        task = tg.create_task(requester(url, settings))
                        task.add_done_callback(lambda t: None)

                        status_code, data = await task
                        if status_code is None or data is None:
                            continue

                        await conn.xadd('responses', '*', {'url': url, 'status_code': status_code, 'content': data})
                        await data_queue.put(data)

                        counter += 1
                except asyncio.TimeoutError:
                    logger.info(f'❌ Timeout while processing URL: {url}')
                except Exception as e:
                    logger.info(
                        f'❌ An exception occurred while processing tasks: {e}')

            if counter >= 10:
                counter = 0

        if debug_mode:
            break

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
