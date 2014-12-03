Worker
======

Worker processes tasks from a queue.

Usage
-----

.. code-block:: shell

    $ kuyruk -app path.to.kuyruk.instance --queue queue_name

If ``queue_name`` is not given default queue name("kuyruk") is used.

OS Signals
----------

SIGINT
    If Kuyruk is run from an interactive shell the first signal initiates a
    warm shutdown. Second signal does a cold shutdown.

    If not run from an interactive shell, it is the same as cold shutdown.

SIGTERM
    Warm shutdown. Exits after current task finishes.

SIGQUIT
    Drops current task and exits immediately. Can be used if the task is stuck.
    The task will not be requeued.

SIGUSR1
    Prints stacktrace. Useful for debugging stuck tasks.
