import os
import logging
import unittest
from time import sleep
from datetime import timedelta
from multiprocessing import Process

from kuyruk.test.integration.util import delete_queue, len_queue, run_scheduler

logger = logging.getLogger(__name__)

DB_FILENAME = "/tmp/kuyruk-scheduler.db"


class SchedulerTestCase(unittest.TestCase):

    def test_scheduler(self):
        """Scheduler schedules correctly"""
        delete_queue('scheduled')

        if os.path.exists(DB_FILENAME):
            os.unlink(DB_FILENAME)

        config = {
            'SCHEDULE': {
                'runs-every-5-seconds': {
                    'task': 'kuyruk.test.tasks.scheduled',
                    'schedule': timedelta(seconds=5),
                    'args': ['hello world from scheduler']
                }
            },
            'SCHEDULER_FILE_NAME': DB_FILENAME
        }

        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        sleep(6)
        p.terminate()
        self.assertEqual(len_queue("scheduled"), 2)

        # checking shelve, this shouldnt send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        p.terminate()
        self.assertEqual(len_queue("scheduled"), 2)

        sleep(2)

        # restart again, now it should send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        sleep(3)
        p.terminate()
        self.assertEqual(len_queue("scheduled"), 3)
