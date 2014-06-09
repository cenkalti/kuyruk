import unittest
from datetime import timedelta
from multiprocessing import Process

from kuyruk.test.integration.util import *
from kuyruk.connection import Channel


Channel.SKIP_REDECLARE_QUEUE = False

logger = logging.getLogger(__name__)

logger.debug('Process id: %s', os.getpid())
logger.debug('Process group id: %s', os.getpgrp())


class SchedulerTestCase(unittest.TestCase):

    def test_scheduler(self):
        """Scheduler schedules correctly"""
        delete_queue('scheduled')

        def get_message_count():
            from kuyruk import Worker
            k = Kuyruk()
            w = Worker(kuyruk=k, queue_name='scheduled')
            w.started = time()
            return w.get_stats()['queue']['messages_ready']

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
        self.assertEqual(get_message_count(), 2)

        # checking shelve, this shouldnt send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        p.terminate()
        self.assertEqual(get_message_count(), 2)

        sleep(2)

        # restart again, now it should send a job
        p = Process(target=run_scheduler, kwargs={'config': config})
        p.start()
        sleep(3)
        p.terminate()
        self.assertEqual(get_message_count(), 3)
