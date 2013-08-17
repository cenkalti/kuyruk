import unittest

from kuyruk import manager, Kuyruk


class ManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.kuyruk = Kuyruk()
        self.manager = manager.Manager(self.kuyruk)
        self.app = self.manager.test_client()

    def test_empty_db(self):
        rv = self.app.get('/')
        # assert '' in rv.data
