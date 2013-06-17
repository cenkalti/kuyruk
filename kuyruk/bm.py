from kuyruk import Kuyruk
from kuyruk.queue import Queue

from time import time
import cProfile, pstats, io

from celery import Celery

pr = cProfile.Profile()


kuyruk = Kuyruk()

@kuyruk.task
def task():
    pass

celery = Celery()

@celery.task
def task2():
    pass

q = Queue('a', None)

t1 = time()
pr.enable()

for i in range(1000):
    print i
    q.send({'a': 1})

t2 = time()

for i in range(1000):
    print i
    task()

t3 = time()

for i in range(1000):
    print i
    task2()

t4 = time()



pr.disable()


s = io.StringIO()
ps = pstats.Stats(pr)
ps.sort_stats('cumulative')
ps.reverse_order()
# ps.print_stats()


print t2-t1
print t3-t2
print t4-t3