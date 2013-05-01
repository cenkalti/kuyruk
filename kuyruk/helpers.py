import threading
from Queue import Empty


def start_daemon_thread(target, args=()):
    t = threading.Thread(target=target, args=args)
    t.daemon = True
    t.start()
    return t


def queue_get_all(q):
    items = []
    while 1:
        try:
            items.append(q.get_nowait())
        except Empty:
            break
    return items
