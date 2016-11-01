import os
import sys
import unittest

from kuyruk.config import Config
from . import config as user_config

sys.path.append(os.path.abspath(os.path.dirname(__file__)))


class ConfigTestCase(unittest.TestCase):

    def test_from_pymodule(self):
        """Config is loaded from Python module"""
        config = Config()
        config.from_pymodule('config2')
        self.assertEqual(config.WORKER_MAX_LOAD, 20)

    def test_from_pyfile(self):
        """Config is loaded from Python file"""
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.py')
        config = Config()
        config.from_pyfile(path)
        self.assertEqual(config.WORKER_MAX_LOAD, 20)

    def test_from_object(self):
        """Config is loaded from Python object"""
        user_config.WORKER_MAX_LOAD = 21
        config = Config()
        config.from_object(user_config)
        self.assertEqual(config.WORKER_MAX_LOAD, 21)

    def test_from_config(self):
        """Config is loaded from dict"""
        config = Config()
        config.from_dict({'WORKER_MAX_LOAD': 21})
        self.assertEqual(config.WORKER_MAX_LOAD, 21)

    def test_from_env_vars(self):
        """Config is laoded from environment variables"""
        os.environ['KUYRUK_WORKER_MAX_LOAD'] = '21'
        config = Config()
        config.from_env_vars()
        self.assertEqual(config.WORKER_MAX_LOAD, 21)
