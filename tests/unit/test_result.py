import json
import logging
import unittest

from kuyruk.result import Result
from kuyruk.exceptions import RemoteException


logger = logging.getLogger(__name__)


class FakeMessage:
    def __init__(self, body):
        self.body = body


class ResultTestCase(unittest.TestCase):

    def test_falsy_results_returned(self):
        """wait() returns falsy results instead of timing out"""
        for value in [0, False, "", [], {}, None]:
            result = Result(connection=None)
            result.process_message(FakeMessage(json.dumps({'result': value})))
            self.assertEqual(result.wait(timeout=1), value)

    def test_truthy_result_returned(self):
        """wait() returns the result payload"""
        result = Result(connection=None)
        result.process_message(FakeMessage(json.dumps({'result': 42})))
        self.assertEqual(result.wait(timeout=1), 42)

    def test_remote_exception_raised(self):
        """wait() raises RemoteException when the reply carries an exception"""
        reply = {'result': None, 'exception': {'type': 'ValueError', 'value': 'boom', 'traceback': ''}}
        result = Result(connection=None)
        result.process_message(FakeMessage(json.dumps(reply)))
        with self.assertRaises(RemoteException):
            result.wait(timeout=1)
