import unittest
from unittest.mock import AsyncMock, Mock, PropertyMock, patch


from apientreprises.main import requester


class TestMain(unittest.IsolatedAsyncioTestCase):
    # @patch('apientreprises.main.redis_connection', Mock)
    # @patch('apientreprises.main.read_from_json', Mock)
    # @patch('apientreprises.main.load_settings_file', Mock)
    # async def test_main(self):
    #     await main()

    async def test_requester(self):
        settings = Mock()
        conf = Mock()

        type(conf).wait_time = PropertyMock(return_value=1)
        type(settings).conf = PropertyMock(return_value=conf)

        status, data = await requester('https://jsonplaceholder.typicode.com/todos/1', settings)
        self.assertEqual(status, 200)
        self.assertIn('userId', data)

    @patch('apientreprises.tasks.clean_data')
    async def test_celery_processor(self, mclean_data):
        with patch('apientreprises.main.data_queue') as mqueue:
            mqueue.empty.return_value = False
            mqueue.get = AsyncMock(return_value={'key': 'value'})

            from apientreprises.main import celery_processor

            await celery_processor(debug_mode=True)
            mqueue.get.assert_awaited()

    async def test_urls_processor(self):
        items = Mock()
        settings = Mock()

        type(settings).conf = PropertyMock(
            return_value=Mock(wait_time=0, iteration_wait_time=0))

        items.pending_urls = [
            'https://jsonplaceholder.typicode.com/todos/1',
            'https://jsonplaceholder.typicode.com/todos/2'
        ]

        with patch('apientreprises.main.redis_connection', AsyncMock()) as mredis:
            mconn = AsyncMock()
            mredis.return_value = mconn
            mconn.xadd = AsyncMock()
            mconn.get = AsyncMock()
            mconn.hset = AsyncMock()

            from apientreprises.main import urls_processor

            await urls_processor(items, settings, debug_mode=True)
            # self.assertEqual(mconn.xadd.await_count, 2)
