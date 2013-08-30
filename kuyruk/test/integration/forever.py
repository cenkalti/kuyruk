import os
import sys
import signal
import subprocess
from time import sleep

from kuyruk import Kuyruk


kuyruk = Kuyruk()


@kuyruk.task(queue='forever')
def run_forever():
    # Execute this script
    path = os.path.abspath(__file__)
    p = subprocess.Popen([sys.executable, path])
    p.wait()
    print "Done."


if __name__ == '__main__':

    # A script that cannot be interrupted

    def handle_signal(signum, frame):
        print 'SIGNAL', signum

    # Ignore all signals
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGABRT, handle_signal)
    signal.signal(signal.SIGALRM, handle_signal)
    signal.signal(signal.SIGQUIT, handle_signal)
    signal.signal(signal.SIGCHLD, handle_signal)
    signal.signal(signal.SIGUSR1, handle_signal)

    # Sleep forever
    i = 0
    while True:
        i += 1
        print i
        sleep(1)
