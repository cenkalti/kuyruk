import os
import logging
import multiprocessing
import math
import json
import traceback
from time import sleep

from . import JobReject
from .queue import Queue

logger = logging.getLogger(__name__)

MAX_LOAD = math.ceil(multiprocessing.cpu_count() * 4)
MAX_RUN_TIME = 60  # seconds


class Worker(object):

    def __init__(self, queue_name, job_handler, sleep_on_load=True, local=False):
        self.queue_name = queue_name
        self.queue = Queue(queue_name, local=local)
        self.job_handler = job_handler
        self._sleep_on_load = sleep_on_load

    def run(self):
        try:
            seconds = MAX_RUN_TIME
            while seconds > 0:
                seconds -= 1

                if self.sleep_on_load():
                    continue

                msg = self.queue.receive_single()
                if not msg:
                    print 'no message available on queue: %s' % self.queue.name
                    sleep(1)
                    continue
                logger.info('got message: %s' % msg)
                kwargs = json.loads(msg)

                try:
                    self.job_handler(**kwargs)
                    self.queue.ack()
                except JobReject:
                    self.queue.reject()  # mesaj tekrar gelsin
                    sleep(1)
        except Exception as e:
            traceback.print_exc()

            self.queue.discard()  # mesaj tekrar gelmesin
            
            # ayri bir kuyruga tekrar atalim dursun,
            # hatayi duzeltince yeniden deneriz
            kwargs['queue'] = self.queue_name
            kwargs['exception'] = traceback.format_exc()
            Queue('failed').send(kwargs)

            sleep(10)  # supervisor basari ile cikti sansin

    def sleep_on_load(self):
        if self._sleep_on_load:
            load = os.getloadavg()
            if load[1] > MAX_LOAD:
                print 'Sleeping because of load... load:%s max_load:%s' % (load[1], MAX_LOAD)
                sleep(1)
                return True


def create_job_handler(cls, fn):
    def handle_job(id=None):
        id = int(id)

        obj = cls.query.get(id)

        if obj:
            logger.info(obj)

            try:
                fn(obj)
            except:
                # raise_if_obj_is_not_deleted
                try:
                    session.commit()
                except:
                    session.rollback()
                obj = cls.query.get(id)
                if obj:
                    raise
        else:
            logger.info('%s(%s) is not found' % (cls.__name__, id))
    return handle_job
