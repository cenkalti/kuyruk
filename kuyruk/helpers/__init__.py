"""
Contains generic functions/classes that can be used in any project.

"""
import logging
import threading
import traceback
from time import sleep
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


def print_stack(sig, frame):
    print '=' * 70
    print ''.join(traceback.format_stack())
    print '-' * 70
