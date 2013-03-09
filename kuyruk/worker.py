import os
import logging
import multiprocessing
import math
import json
import traceback
from time import sleep

from kuyruk.loader import import_task

logger = logging.getLogger(__name__)

MAX_LOAD = math.ceil(multiprocessing.cpu_count() * 4)
MAX_RUN_TIME = 60  # seconds


class Worker(object):

    RESULT_OK = 0
    RESULT_ERROR = 1
    RESULT_RECECT = 2

    def __init__(self, in_queue, out_queue):
        self.in_queue = in_queue
        self.out_queue = out_queue

    def work(self):
        tag, job = self.in_queue.get()
        logger.info('got message: %s', job)

        try:
            fname = job['fname']
            args = job['args']
            kwargs = job['kwargs']

            task = import_task(fname)
            logger.debug(
                'Task %r will be executed with args=%r and kwargs=%r',
                task, args, kwargs)

            result = task.f(*args, **kwargs)
            logger.debug('Result: %r', result)

            self.out_queue.put((tag, Worker.RESULT_OK))
        except Exception:
            print '*' * 80
            traceback.print_exc()
            self.out_queue.put((tag, Worker.RESULT_ERROR))

    def run(self):
        try:
            seconds = MAX_RUN_TIME
            while seconds > 0:
                seconds -= 1

                # if self.sleep_on_load():
                #     continue

                delivery_tag, job = self.queue.receive()
                if not job:
                    print 'no message available on queue: %s' % self.queue.name
                    sleep(1)
                    continue
                logger.info('got message: %s', job)

                fname = job['fname']
                args = job['args']
                kwargs = job['kwargs']

                # try:
                #     self.job_handler(**kwargs)
                #     self.queue.ack()
                # except JobReject:
                #     self.queue.reject()  # mesaj tekrar gelsin
                #     sleep(1)
        except Exception:
            traceback.print_exc()

            self.queue.discard()  # mesaj tekrar gelmesin
            
            # ayri bir kuyruga tekrar atalim dursun,
            # hatayi duzeltince yeniden deneriz
            kwargs['queue'] = self.queue_name
            kwargs['exception'] = traceback.format_exc()
            Queue('failed').send(kwargs)

            sleep(10)  # supervisor basari ile cikti sansin

    # def sleep_on_load(self):
    #     if self._sleep_on_load:
    #         load = os.getloadavg()
    #         if load[1] > MAX_LOAD:
    #             print 'Sleeping because of load... load:%s max_load:%s' % (load[1], MAX_LOAD)
    #             sleep(1)
    #             return True


# def create_job_handler(cls, fn):
#     def handle_job(id=None):
#         id = int(id)
#
#         obj = cls.query.get(id)
#
#         if obj:
#             logger.info(obj)
#
#             try:
#                 fn(obj)
#             except:
#                 # raise_if_obj_is_not_deleted
#                 try:
#                     session.commit()
#                 except:
#                     session.rollback()
#                 obj = cls.query.get(id)
#                 if obj:
#                     raise
#         else:
#             logger.info('%s(%s) is not found' % (cls.__name__, id))
#     return handle_job
