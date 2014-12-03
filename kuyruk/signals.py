"""
In signals starting with ``task_`` the sender is
Task instance and Kuyruk instance.

In signals starting with ``worker_`` the sender is
Worker instance and Kuyruk instance.

"""
from blinker import Signal

#: Sent when the task decorator is applied.
task_init = Signal()

#: Sent before the task is applied.
task_preapply = Signal()

#: Sent after the task is applied.
task_postapply = Signal()

#: Sent before the wrapped function is executed.
task_prerun = Signal()

#: Sent after the wrapped function is executed.
task_postrun = Signal()

#: Sent when the wrapped function is returned.
task_success = Signal()

#: Sent when the wrapped function raises an exception.
task_error = Signal()

#: Sent when the task fails after all retries(if any).
task_failure = Signal()

#: Sent before the task is sent to queue.
task_presend = Signal()

#: Sent after the task is sent to queue.
task_postsend = Signal()

#: Sent when the task fails.
worker_failure = Signal()

#: Sent when the worker is initialized.
worker_init = Signal()

#: Sent when the worker shuts down.
worker_shutdown = Signal()
