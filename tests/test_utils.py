from unittest.mock import mock_open, patch

import pytest
from faker import Faker

from apientreprises.main import read_from_json
from apientreprises.main import load_settings_file

fake = Faker()


@pytest.fixture
def fake_data():
    return {'pages': fake.random_number(1, 1000)}


@pytest.mark.asyncio
async def test_load_settings_file():
    result = await load_settings_file()
    assert result is not None
    assert hasattr(result, 'conf')
    assert hasattr(result.conf, 'wait_time')
    assert hasattr(result.conf, 'iteration_wait_time')


@pytest.mark.asyncio
async def test_read_from_json(fake_data):
    with patch('apientreprises.main.orjson') as morjson:
        morjson.loads.return_value = fake_data

        with patch('builtins.open', mock_open(read_data=b'{"key": "value"}')):
            await read_from_json('testfile')


@pytest.mark.asyncio
async def test_write_to_json():
    pass
