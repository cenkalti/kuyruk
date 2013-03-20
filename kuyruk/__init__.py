import imp
import logging
import optparse

from .kuyruk import Kuyruk
from .task import Task
from .worker import Worker
from .queue import Queue

logger = logging.getLogger(__name__)


def main():
    # from worker import Worker
    # from worker import create_job_handler

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('pika').level = logging.WARNING

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    parser.add_option('-r', '--max-run-time', type='int')
    # parser.add_option('-w', '--workers', type='int')
    # parser.add_option("-l", "--local",
    #                   action="store_true", default=False,
    #                   help="append hostname to queue name")
    # sleep on load
    # max load
    # ayri queue

    options, args = parser.parse_args()

    if not args:
        args = ['kuyruk']

    if options.config:
        config = imp.load_source('config', options.config)
    else:
        config = imp.new_module('config')

    if options.max_run_time:
        config.KUYRUK_MAX_RUN_TIME = options.max_run_time

    kuyruk = Kuyruk(config=config)
    kuyruk.run(args[0])

    # queue = model.lower() + '_' + method
    # module = __import__('putio.models', globals(), locals(), [model])
    # cls = getattr(module, model)
    # requirements_fn = getattr(cls, method + '_requirements', None)
    # if requirements_fn:
    #     requirements_fn()
    # fn = getattr(cls, method)
    # job_handler = create_job_handler(cls, fn)
    # Worker(queue, job_handler, local=options.local).run()



    # def sleep_on_load(self):
    #     if self._sleep_on_load:
    #         load = os.getloadavg()
    #         if load[1] > MAX_LOAD:
    #             print 'Sleeping because of load... load:%s max_load:%s' % (load[1], MAX_LOAD)
    #             sleep(1)
    #             return True
