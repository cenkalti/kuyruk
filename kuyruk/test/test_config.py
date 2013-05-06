import os
import unittest

from ..config import Config
import config as user_config


class ConfigTestCase(unittest.TestCase):

    def test_from_pyfile(self):
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.py')
        config = Config()
        config.from_pyfile(path)
        self.assertEqual(config.MAX_LOAD, 20)

    def test_from_object(self):
        user_config.KUYRUK_MAX_LOAD = 21
        config = Config()
        config.from_object(user_config)
        self.assertEqual(config.MAX_LOAD, 21)
