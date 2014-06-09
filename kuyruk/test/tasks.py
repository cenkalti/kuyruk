"""
Contains sample tasks to be used in unit and integration tests.

"""

import sys
import string
import random
from time import sleep

from kuyruk import Kuyruk, Task
from kuyruk.events import task_prerun, task_postrun


kuyruk = Kuyruk()
# These functions below needs to be at module level in order that
# Kuyruk worker to determine their fully qualified name.


@kuyruk.task
def print_task(message):
    print message
    must_be_called()


@kuyruk.task(queue='another_queue')
def print_task2(message):
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


@kuyruk.task(eager=True)
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
def task_with_functions(message):
    print message
    return 42


@kuyruk2.on_presend
def function0(sender, task, args, kwargs):
    must_be_called()


@kuyruk2.on_prerun
def function1(sender, task, args, kwargs):
    print 'function1'
    print sender, task, args, kwargs
    assert sender is kuyruk2
    assert isinstance(task, Task)
    assert list(args) == ['hello world']
    assert kwargs == {}


@task_with_functions.on_prerun
def function2(sender, task, args, kwargs):
    print 'function2'


@task_with_functions.on_success
def function3(sender, task, args, kwargs, return_value):
    print 'function3'
    assert return_value == 42


@task_with_functions.on_postrun
def function4(sender, task, args, kwargs):
    print 'function4'


@kuyruk2.on_postrun
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

    @kuyruk.task(eager=True)
    def meow_eager(self, message):
        print "Felix says:", message
        must_be_called()

    @kuyruk.task
    def raise_exception(self):
        raise Exception


@kuyruk.task(arg_class=Cat)
def jump(cat):
    print "%s jumps high!" % cat.name
    must_be_called(cat.name)


@kuyruk.task(arg_class=Cat, eager=True)
def jump_eager(cat):
    print "%s jumps high!" % cat.name
    must_be_called(cat.name)


@kuyruk.task(arg_class=Cat)
def jump_fail(cat):
    1/0


def must_be_called(arg=None):
    """
    This function is patched in tests to see the caller is doing it's job.

    """
    print 'Yes, it is called.'
    print 'Called with %s' % arg


class DatabaseTask(Task):

    def setup(self):
        self.connect_signal(task_prerun, self.open_session)
        self.connect_signal(task_postrun, self.close_session)

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


@kuyruk.task(queue='scheduled')
def scheduled(message):
    print message
