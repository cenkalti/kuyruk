import os
import unittest

from ..config import Config
import config as user_config


class ConfigTestCase(unittest.TestCase):

    def test_init_from_file(self):
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.py')
        config = Config.from_path(path)
        self.assertEqual(config.MAX_LOAD, 20)

    def test_init_from_module(self):
        user_config.KUYRUK_MAX_LOAD = 21
        config = Config(user_config)
        self.assertEqual(config.MAX_LOAD, 21)
