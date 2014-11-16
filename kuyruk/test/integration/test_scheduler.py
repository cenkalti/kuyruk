import unittest
from datetime import timedelta
from multiprocessing import Process

from kuyruk.test.integration.util import *


logger = logging.getLogger(__name__)


class SchedulerTestCase(unittest.TestCase):

    def test_scheduler(self):
        """Scheduler schedules correctly"""
        delete_queue('scheduled')

        if os.path.exists('/tmp/kuyruk-scheduler.db'):
            os.unlink('/tmp/kuyruk-scheduler.db')

        config = {
            'SCHEDULE': {
                'runs-every-5-seconds': {
                    'task': 'kuyruk.test.tasks.scheduled',
                    'schedule': timedelta(seconds=5),
                    'args': ['hello world from scheduler']
                }
            },
            'SCHEDULER_FILE_NAME': '/tmp/kuyruk-scheduler'
        }

        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        sleep(6)
        p.terminate()
        self.assertEqual(messages_ready('scheduled'), 2)

        # checking shelve, this shouldnt send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        p.terminate()
        self.assertEqual(messages_ready('scheduled'), 2)

        sleep(2)

        # restart again, now it should send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        sleep(3)
        p.terminate()
        self.assertEqual(messages_ready('scheduled'), 3)
