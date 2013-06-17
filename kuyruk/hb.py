import logging
from kuyruk import Kuyruk
from time import sleep

logging.basicConfig(level=logging.DEBUG)

kuyruk = Kuyruk()


@kuyruk.task
def task():
    pass


task()

i = 0
while True:
    print i
    i += 1
    try:
        sleep(1)
    except:
        break


task()

