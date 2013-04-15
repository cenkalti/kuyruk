from time import sleep

from ..kuyruk import Kuyruk
from ..exceptions import Reject


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


@kuyruk.task(eager=True)
def eager_task():
    eager_called.append(1)
eager_called = []


@kuyruk.task
def rejecting_task():
    raise Reject


@kuyruk.task
def sleeping_task(seconds):
    sleep(seconds)
