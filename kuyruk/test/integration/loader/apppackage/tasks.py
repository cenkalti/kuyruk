from kuyruk import Kuyruk

kuyruk = Kuyruk()


@kuyruk.task
def print_message(m):
    print m
