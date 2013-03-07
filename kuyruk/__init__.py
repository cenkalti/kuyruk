import sys
import logging
import optparse

from task import task

logger = logging.getLogger(__name__)


class JobReject(Exception):
    pass


class Kuyruk(object):

    def __init__(self, name, num_workers=1, config_path=None, local=False):
        if local:
            assert host is None

        self.name = name
        self.config_path = config_path
        self.num_workers = int(num_workers)
        assert self.num_workers > 0
        self.local = local

        if local:
            self.name = '%s_%s' % (self.name, socket.gethostname())


def main():
    from worker import Worker
    from worker import create_job_handler

    logging.basicConfig(level=logging.DEBUG)
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config')
    parser.add_option('-w', '--workers', type='int')
    parser.add_option("-l", "--local",
                      action="store_true", default=False,
                      help="append hostname to queue name")
    options, args = parser.parse_args()

    kuyruk = Kuyruk(
        num_workers=options.workers,
        config_path=options.config,
        local=options.local
    )

    sys.exit()

    queue = model.lower() + '_' + method
    module = __import__('putio.models', globals(), locals(), [model])
    cls = getattr(module, model)
    requirements_fn = getattr(cls, method + '_requirements', None)
    if requirements_fn:
        requirements_fn()
    fn = getattr(cls, method)
    job_handler = create_job_handler(cls, fn)
    Worker(queue, job_handler, local=options.local).run()
