import logging

import unittest

from kuyruk.test import tasks


logger = logging.getLogger(__name__)


class TaskTestCase(unittest.TestCase):

    def test_invalid_call(self):
        """Invalid call of task raises exception"""
        self.assertRaises(TypeError, tasks.jump)
        self.assertRaises(TypeError, tasks.jump, object())
        felix = tasks.Cat(1, 'felix')
        del felix.id
        self.assertRaises(AttributeError, tasks.jump, felix)
