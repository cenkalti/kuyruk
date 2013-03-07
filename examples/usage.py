from kuyruk import Kuyruk
import config

kuyruk = Kuyruk(config)

@kuyruk.task
def echo(message):
    print message

# runs in background
echo('hello world')
