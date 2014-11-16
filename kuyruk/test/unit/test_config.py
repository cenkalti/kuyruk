import os
import unittest

from kuyruk.config import Config
from kuyruk.test.unit import config as user_config


class ConfigTestCase(unittest.TestCase):

    def test_from_pymodule(self):
        config = Config()
        config.from_pymodule('kuyruk.test.unit.config2')
        self.assertEqual(config.MAX_LOAD, 20)

    def test_from_pyfile(self):
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.py')
        config = Config()
        config.from_pyfile(path)
        self.assertEqual(config.MAX_LOAD, 20)

    def test_from_object(self):
        user_config.MAX_LOAD = 21
        config = Config()
        config.from_object(user_config)
        self.assertEqual(config.MAX_LOAD, 21)
