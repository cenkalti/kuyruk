"""
Contains sample tasks to be used in unit and integration tests.

"""

import sys
import string
import random
from time import sleep

from kuyruk import Kuyruk, Task, Config
from kuyruk import signals


kuyruk = Kuyruk()
# These functions below needs to be at module level in order that
# Kuyruk worker to determine their fully qualified name.


@kuyruk.task
def echo(message):
    print message
    must_be_called()


@kuyruk.task(queue='another_queue')
def echo_another(message):
    print message


@kuyruk.task
def flood():
    s = ''.join(random.choice(string.ascii_uppercase) for _ in xrange(70))
    while True:
        sys.stdout.write(s)
        sys.stdout.write('\n')
        sys.stdout.flush()
        sys.stderr.write(s)
        sys.stderr.write('\n')
        sys.stderr.flush()


@kuyruk.task
def raise_exception():
    return 1 / 0


@kuyruk.task(retry=1)
def retry_task():
    return 1 / 0


@kuyruk.task
def loop_forever():
    while 1:
        print 'looping forever'
        sleep(1)


config_eager = Config()
config_eager.EAGER = True
kuyruk_eager = Kuyruk(config_eager)
@kuyruk_eager.task()
def eager_task():
    must_be_called()


@kuyruk.task
def rejecting_task():
    raise kuyruk.Reject


@kuyruk.task(max_run_time=1)
def sleeping_task(seconds):
    sleep(seconds)


# Another kuyruk instance for testing before and after task decorators
kuyruk2 = Kuyruk()


@kuyruk2.task
def task_with_signal_handlers(message):
    print message
    return 42


@signals.task_presend.connect_via(kuyruk2)
def function0(sender, task, args, kwargs, description):
    must_be_called()


@signals.task_prerun.connect_via(task_with_signal_handlers)
def function1(sender, task, args, kwargs):
    print 'function1'


@signals.task_prerun.connect_via(kuyruk2)
def function2(sender, task, args, kwargs):
    print 'function2'
    print sender, task, args, kwargs
    assert sender is kuyruk2
    assert isinstance(task, Task)
    assert list(args) == ['hello world']
    assert kwargs == {}


@signals.task_success.connect_via(task_with_signal_handlers)
def function3(sender, task, args, kwargs):
    print 'function3'


@signals.task_postrun.connect_via(task_with_signal_handlers)
def function4(sender, task, args, kwargs):
    print 'function4'


@signals.task_postrun.connect_via(kuyruk2)
def function5(sender, task, args, kwargs):
    print 'function5'


class Cat(object):

    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "Cat(%r, %r)" % (self.id, self.name)

    @classmethod
    def get(cls, id):
        if id == 1:
            return cls(1, 'Felix')

    @kuyruk.task
    def meow(self, message):
        print "Felix says:", message
        must_be_called()

    @kuyruk.task
    def raise_exception(self):
        raise Exception


@kuyruk.task(arg_class=Cat)
def jump(cat):
    print "%s jumps high!" % cat.name
    must_be_called(cat.name)


@kuyruk.task(arg_class=Cat)
def jump_error(cat):
    1/0


def must_be_called(arg=None):
    """
    This function is patched in tests to see the caller is doing it's job.

    """
    print 'Yes, it is called.'
    print 'Called with %s' % arg


class DatabaseTask(Task):

    def setup(self):
        signals.task_prerun.connect(self.open_session, sender=self, weak=False)
        signals.task_postrun.connect(self.close_session, sender=self, weak=False)

    def open_session(self, sender, task, args, kwargs):
        print 'Opening session'
        self.session = object()

    def close_session(self, sender, task, args, kwargs):
        print 'Closing session'
        self.session = None


@kuyruk.task(task_class=DatabaseTask)
def use_session():
    print use_session.session


@kuyruk.task
def spawn_process(args=['sleep', '60']):
    import subprocess
    subprocess.check_call(args)
