import os
import sys
import unittest

# HACK: Prepend ../ to PYTHONPATH so that we can import muhtar form there.
TESTS_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.realpath(os.path.join(TESTS_ROOT, '..')))

from kuyruk import Kuyruk


class KuyrukTestCase(unittest.TestCase):

    def test_config(self):
