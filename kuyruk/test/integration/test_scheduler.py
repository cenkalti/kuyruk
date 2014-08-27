import os
import logging
import unittest
from time import sleep

import rabbitpy

from kuyruk import Kuyruk
from kuyruk.test.integration.util import run_kuyruk

logger = logging.getLogger(__name__)

DB_FILENAME = "/tmp/kuyruk-scheduler.db"
CONFIG_FILENAME = "/tmp/kuyruk-config.py"


class SchedulerTestCase(unittest.TestCase):

    def setUp(self):
        self.kuyruk = Kuyruk()
        self.queue = rabbitpy.Queue(self.kuyruk.channel(), "scheduled", durable=True)
        self.queue.delete()

    def test_scheduler(self):
        """Scheduler schedules correctly"""
        if os.path.exists(DB_FILENAME):
            os.unlink(DB_FILENAME)

        config = """\
from datetime import timedelta

SCHEDULER_FILE_NAME = "%s"
SCHEDULE = {
    'runs-every-5-seconds': {
        'task': 'kuyruk.test.tasks.scheduled',
        'schedule': timedelta(seconds=5),
        'args': ['hello world from scheduler']
    }
}
""" % DB_FILENAME
        with open(CONFIG_FILENAME, "w+") as f:
            f.write(config)

        with run_kuyruk(process="scheduler", config_filename=CONFIG_FILENAME) as p:
            p.expect("Start loop")
            p.expect("sending due task", timeout=1)
            p.expect("sending due task", timeout=6)
        assert len(self.queue) == 2

        # checking shelve, this shouldnt send a job
        with run_kuyruk(process="scheduler", config_filename=CONFIG_FILENAME) as p:
            p.expect("Start loop")
            p.expect("last run of runs-every-5-seconds")
            sleep(2)
        assert len(self.queue) == 2

        # restart again, now it should send a job
        with run_kuyruk(process="scheduler", config_filename=CONFIG_FILENAME) as p:
            p.expect("Start loop")
            p.expect("sending due task", timeout=5)
        assert len(self.queue) == 3
