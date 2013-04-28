from time import sleep

from kuyruk.kuyruk import Kuyruk


kuyruk = Kuyruk()
# These functions below needs to be at module level in order that
# Kuyruk worker to determine their fully qualified name.


@kuyruk.task
def print_task(message):
    print message


@kuyruk.task(queue='another_queue')
def print_task2(message):
    print message


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
    eager_called.append(1)
eager_called = []


@kuyruk.task
def rejecting_task():
    raise kuyruk.Reject


@kuyruk.task
def sleeping_task(seconds):
    sleep(seconds)


# Another kuyruk instance for testing before and after task decorators
kuyruk2 = Kuyruk()


@kuyruk2.task
def task_with_functions(message):
    print message


@kuyruk2.before_task
def function1(task, args, kwargs):
    print 'function1'


@task_with_functions.before_task
def function2(task, args, kwargs):
    print 'function2'


@task_with_functions.after_task
def function3(task, args, kwargs):
    print 'function3'


@kuyruk2.after_task
def function4(task, args, kwargs):
    print 'function4'


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
