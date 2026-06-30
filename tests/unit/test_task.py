import time
import logging
import unittest

from kuyruk import Kuyruk, signals
from kuyruk.task import time_limit
from kuyruk.exceptions import Timeout


logger = logging.getLogger(__name__)


class TaskApplyTestCase(unittest.TestCase):

    def test_success_signal_sent(self):
        """task_success is sent when a task returns successfully"""
        k = Kuyruk()

        @k.task()
        def add(a, b):
            return a + b

        received = []
        with signals.task_success.connected_to(lambda sender, **kw: received.append(kw['task']), sender=k):
            result = add.apply(2, 3)

        self.assertEqual(result, 5)
        self.assertEqual(received, [add])

    def test_success_signal_not_sent_on_failure(self):
        """task_failure is sent and task_success is not when a task raises"""
        k = Kuyruk()

        @k.task()
        def fail():
            raise ValueError("boom")

        success, failure = [], []
        with signals.task_success.connected_to(lambda s, **kw: success.append(1), sender=k), \
                signals.task_failure.connected_to(lambda s, **kw: failure.append(1), sender=k):
            with self.assertRaises(ValueError):
                fail.apply()

        self.assertEqual(success, [])
        self.assertEqual(failure, [1])


class TimeLimitTestCase(unittest.TestCase):

    def test_float_timeout(self):
        """time_limit accepts float seconds and raises Timeout"""
        with self.assertRaises(Timeout):
            with time_limit(0.1):
                time.sleep(5)

    def test_zero_disables_limit(self):
        """time_limit with zero seconds does not arm a timer"""
        with time_limit(0):
            pass
