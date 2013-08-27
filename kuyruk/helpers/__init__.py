"""
Contains generic functions/classes that can be used in any project.

"""
import errno
import select
import logging
import threading
import traceback
from time import sleep
from Queue import Empty
from functools import wraps


logger = logging.getLogger(__name__)


def start_daemon_thread(target, args=()):
    t = threading.Thread(target=target, args=args)
    t.daemon = True
    t.start()
    return t


def retry(sleep_seconds=1, stop_event=threading.Event(),
          on_exception=lambda e: logger.debug(e)):
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            while not stop_event.is_set():
                try:
                    f(*args, **kwargs)
                except Exception as e:
                    if on_exception:
                        on_exception(e)
                    if sleep_seconds:
                        sleep(sleep_seconds)
        return inner
    return decorator


def queue_get_all(q):
    items = []
    while 1:
        try:
            items.append(q.get_nowait())
        except Empty:
            break
    return items


def human_time(seconds, suffixes=['y', 'w', 'd', 'h', 'm', 's'],
               add_s=False, separator=' '):
    """
    Takes an amount of seconds and
    turns it into a human-readable amount of time.

    """
    # the formatted time string to be returned
    time = []

    # the pieces of time to iterate over (days, hours, minutes, etc)
    # - the first piece in each tuple is the suffix (d, h, w)
    # - the second piece is the length in seconds (a day is 60s * 60m * 24h)
    parts = [
        (suffixes[0], 60 * 60 * 24 * 7 * 52),
        (suffixes[1], 60 * 60 * 24 * 7),
        (suffixes[2], 60 * 60 * 24),
        (suffixes[3], 60 * 60),
        (suffixes[4], 60),
        (suffixes[5], 1)]

    # for each time piece, grab the value and remaining seconds, and add it to
    # the time string
    for suffix, length in parts:
        value = seconds / length
        if value > 0:
            seconds %= length
            time.append('%s%s' % (str(value),
                        (suffix, (suffix, suffix + 's')[value > 1])[add_s]))
        if seconds < 1:
            break

    return separator.join(time)


def monkeypatch_method(cls):
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func
    return decorator


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70


def retry_on_eintr(f):
    @wraps(f)
    def inner(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except (select.error, OSError) as e:
                if e[0] != errno.EINTR:
                    raise
    return inner
