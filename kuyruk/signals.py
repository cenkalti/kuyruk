from blinker import Signal

#: Sent when the task decorator is applied.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
task_init = Signal()

#: Sent before the task is applied.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
task_preapply = Signal()

#: Sent after the task is applied.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
task_postapply = Signal()

#: Sent before the wrapped function is executed.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
task_prerun = Signal()

#: Sent after the wrapped function is executed.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
task_postrun = Signal()

#: Sent when the wrapped function is returned.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
task_success = Signal()

#: Sent when the wrapped function raises an exception.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
#:    * exc_info: Return value of ``sys.exc_info()``
task_error = Signal()

#: Sent when the task fails after all retries(if any).
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
#:    * exc_info: Return value of ``sys.exc_info()``
task_failure = Signal()

#: Sent before the task is sent to queue.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
#:    * description: dict representation of the task
task_presend = Signal()

#: Sent after the task is sent to queue.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
#:    * description: dict representation of the task
task_postsend = Signal()

#: Sent when the task fails.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * worker: The Worker object
#:    * task: Task object
#:    * args: Positional arguments to the task
#:    * kwargs: Keyword arguments to the task
#:    * description: dict representation of the task
#:    * exc_info: Return value of ``sys.exc_info()``
worker_failure = Signal()

#: Sent when the worker is initialized.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * worker: The Worker object
worker_init = Signal()

#: Sent when the worker is started.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * worker: The Worker object
worker_start = Signal()

#: Sent when the worker shuts down.
#:
#: Arguments:
#:    * sender: Kuyruk object
#:    * worker: The Worker object
worker_shutdown = Signal()
