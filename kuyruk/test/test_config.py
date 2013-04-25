import unittest

from ..config import Config


class ConfigTestCase(unittest.TestCase):

    def test_dict(self):
        c = Config({'KUYRUK_RABBIT_PORT': 1})
        self.assertEqual(c.RABBIT_PORT, 1)

    def test_obj(self):
        class C(object):
            pass
        obj = C()
        obj.KUYRUK_RABBIT_PORT = 1
        c = Config(obj)
        self.assertEqual(c.RABBIT_PORT, 1)
