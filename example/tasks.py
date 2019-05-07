from __future__ import print_function
from kuyruk import Kuyruk

kuyruk = Kuyruk()


@kuyruk.task()
def echo(message):
    print(message)
