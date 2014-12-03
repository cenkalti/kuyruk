import os
import sys
import unittest

from kuyruk.config import Config
from . import config as user_config

sys.path.append(os.path.abspath(os.path.dirname(__file__)))


class ConfigTestCase(unittest.TestCase):

    def test_from_pymodule(self):
        config = Config()
        config.from_pymodule('config2')
        self.assertEqual(config.WORKER_MAX_LOAD, 20)

    def test_from_pyfile(self):
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.py')
        config = Config()
        config.from_pyfile(path)
        self.assertEqual(config.WORKER_MAX_LOAD, 20)

    def test_from_object(self):
        user_config.WORKER_MAX_LOAD = 21
        config = Config()
        config.from_object(user_config)
        self.assertEqual(config.WORKER_MAX_LOAD, 21)
