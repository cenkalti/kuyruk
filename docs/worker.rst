Worker
======

Worker processes tasks from a queue.

Usage
-----

.. code-block:: shell

    $ kuyruk --app <path.to.kuyruk.instance> --queue <queue_name>

If ``queue_name`` is not given default queue name("kuyruk") is used.

Example:

.. code-block:: shell

    $ kuyruk --app tasks.kuyruk --queue download_file

OS Signals
----------

Description of how worker processes react to OS signals.

SIGINT
    Worker exits after completing the current task.

    *This is the signal sent when you press CTRL-C on your keyboard.*

SIGTERM
    Worker exits after completing the current task.

SIGQUIT
    Worker quits immediately. This is unclean shutdown.
    If worker is running a task it will be requeued by RabbitMQ.

    *This is the signal sent when you press CTRL-\ on your keyboard.*

SIGUSR1
    Prints stacktrace. Useful for debugging stuck tasks or seeing what the
    worker is doing.

SIGUSR2
    Discard current task and proceed to next one.
    Discarded task will not be requeued by RabbitMQ.

SIGHUP
    Used internally to fail the task when connection to RabbitMQ is
    lost during the execution of a long running task. Do not use it.
