import asyncio
import dataclasses
import uuid

import fastapi
import httpx
from urllib.parse import urlencode
import subprocess

import pydantic
from fastapi import BackgroundTasks
from apientreprises import logger
from apientreprises import tasks as celery_tasks
from apientreprises.models import DataModel, SettingsModel, UrlsModel
from apientreprises.utils import (load_settings_file, read_from_json,
                                  redis_connection)

app = fastapi.FastAPI(
    debug=True,
    title='API Enterprises Processor',
    description='An asynchronous processor for fetching and handling enterprise data from APIs.',
    version='1.0.0'
)

data_queue = asyncio.Queue()

event = asyncio.Event()


async def requester(url: str, settings: SettingsModel) -> tuple[int | None,  DataModel | None]:
    """Makes an asynchronous HTTP GET request to the specified URL
    and returns the status code and JSON response.

    Args:
        url (str): The URL to request.
        settings (SettingsModel): The settings model with configuration.
    """
    # Some API endpoints may have rate limits; respect them
    await asyncio.sleep(settings.conf.wait_time)

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
        return response.status_code, DataModel(**response.json())


async def file_processor(debug_mode: bool = False):
    """The file processor continuously checks the data queue for new data
    and processes it accordingly by creating the necessary Celery tasks
    for file creation, uploading, etc.
    """
    # await event.wait()

    while True:
        if not data_queue.empty():
            data = await data_queue.get()

            # Process the data (e.g., save to database, further analysis, etc.)
            logger.info(f'⚪️ Processing data: {data}')
            celery_tasks.clean_data.apply_async(args=[data])

        if data_queue.empty() and not event.is_set():
            logger.info('✅ No more data to process. Exiting file processor.')
            break

        if debug_mode:
            break

        await asyncio.sleep(10)


async def urls_processor(items: UrlsModel, settings: SettingsModel, debug_mode: bool = False):
    """The URLs processor continuously processes pending URLs, fetching data
    from each URL and storing the results in Redis.

    Args:
        items (UrlsModel): The URLs model containing pending and done URLs.
        settings (SettingsModel): The settings model with configuration.
        debug_mode (bool, optional): If True, runs in debug mode and exits after one iteration. Defaults to False.
    """
    conn = await redis_connection()

    event.set()

    while items.pending_urls:
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

                        status_code, data_model = await task
                        if status_code is None or data_model is None:
                            continue

                        validated_data = data_model.model_dump_json()

                        await conn.xadd('responses', '*', {'url': url, 'status_code': status_code, 'content': validated_data})
                        await data_queue.put(validated_data)

                        counter += 1

                        # chain_instance = chain(
                        #     celery_tasks.clean_data.s(validated_data),
                        #     celery_tasks.create_file.s(),
                        #     celery_tasks.upload_to_storage.s()
                        # )

                        # chain_instance.apply_async(
                        #     eta=datetime.datetime.now() + datetime.timedelta(seconds=10)
                        # )

                        items.done_urls.append(url)
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

    event.clear()


async def main(name: str):
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
    settings_model = await load_settings_file()

    params = {
        'q': name,
        'page': 1,
        'per_page': settings_model.conf.per_page
    }

    query_string = urlencode(params)
    search_url = f"{settings_model.conf.search_url}?{query_string}"

    status, initial = await requester(search_url, settings_model)
    if status is None or initial is None:
        raise SystemExit('Failed to fetch initial data.')

    urls = UrlsModel()

    for i in range(initial.total_pages):
        params['page'] = i + 1
        query_string = urlencode(params)
        page_url = f"{settings_model.conf.search_url}?{query_string}"
        urls.pending_urls.append(page_url)

    await conn.hset(runid, 'urls', bytes(str(dataclasses.asdict(urls)).encode('utf-8')))

    logger.info(f'🔑 Run ID: {runid}')
    logger.info(f'📥 Pending URLs: {len(urls.pending_urls)}')

    t1 = asyncio.create_task(urls_processor(urls, settings_model))
    t2 = asyncio.create_task(file_processor())
    await asyncio.gather(t1, t2)


def start_celery():
    try:
        subprocess.Popen([
            'celery',
            '-A',
            'apientreprises.celery_app',
            'worker',
            '-E',
            '--loglevel=info'
        ])
        logger.info('✅ Celery worker started successfully.')
    except Exception as e:
        logger.error(f'❌ Error starting Celery worker: {e}')


class PostBody(pydantic.BaseModel):
    name: str


class ResponseModel(pydantic.BaseModel):
    message: str
    errors: list[str] | None = None


@app.post('/search')
async def search(body: PostBody, background_tasks: BackgroundTasks) -> ResponseModel:
    background_tasks.add_task(main, body.name)
    return ResponseModel(message='Search initiated successfully.', errors=None)


# if __name__ == '__main__':
#     # try:
#     #     t = threading.Thread(target=start_celery)
#     #     t.start()
#     # except Exception as e:
#     #     logger.error(f'❌ Failed to start Celery worker: {e}')
#     #     sys.exit(1)
#     # else:
#     #     t.join()

#     uvicorn.run(app, host='0.0.0.0', port=8000)

# # if __name__ == '__main__':
# #     try:
# #         asyncio.run(main())
# #     except Exception as e:
# #         print(f'Error: {e}')
# #     except KeyboardInterrupt:
# #         logger.info('✅ Process interrupted by user.')
