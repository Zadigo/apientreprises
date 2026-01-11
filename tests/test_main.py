import unittest
from unittest.mock import Mock, mock_open, patch
from apientreprises.main import main

import pytest
from faker import Faker
from faker.factory import Factory

from apientreprises.main import read_from_json, write_to_json


class TestMain(unittest.IsolatedAsyncioTestCase):
    @patch('apientreprises.main.redis_connection', Mock)
    @patch('apientreprises.main.read_from_json', Mock)
    @patch('apientreprises.main.load_settings_file', Mock)
    async def test_main(self):
        await main()
