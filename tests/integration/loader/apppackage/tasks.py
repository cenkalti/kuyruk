from __future__ import print_function
from kuyruk import Kuyruk, Config

config = Config()
config.from_pyfile('/tmp/kuyruk_config.py')

kuyruk = Kuyruk(config=config)


@kuyruk.task
def print_message(m):
    print(m)
