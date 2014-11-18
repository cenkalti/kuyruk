Signals
=======

This section describes how Kuyruk workers react to OS signals.


SIGINT
    If Kuyruk is run from an interactive shell the first signal initiates a
    warm shutdown. Second signal does a cold shutdown.

    If not run from an interactive shell, it is the same as cold shutdown.

SIGTERM
    Warm shutdown. Exits after current tasks finishes.

SIGQUIT
    Drops current task and exits immediately. Can be used if the task is stuck.

SIGUSR1
    Prints stacktrace. Useful for debugging stuck processes.
