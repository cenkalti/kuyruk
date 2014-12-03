from __future__ import print_function
from kuyruk import Kuyruk

kuyruk = Kuyruk()


@kuyruk.task
def print_message(m):
    print(m)


if __name__ == '__main__':
    print_message('asdf')
