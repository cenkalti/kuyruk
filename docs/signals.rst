Signals
=======

The following sections describe that how will Kuyruk processes react to signals.


Master Process
--------------

SIGTERM
    Warm shutdown: Shutdown workers gracefully and exit.

SIGQUIT
    Cold shutdown: Kill workers and exit.

SIGABRT
    Die immediately. Workers will detect that their
    master is dead and will initiate a warm shutdown.

SIGINT
    If Kuyruk is run from an interactive shell the first signal initiates a
    warm shutdown (same as SIGTERM). Second signal does a cold shutdown
    (same as SIGQUIT).

    If not run from an interactive shell, it is the same as SIGQUIT.

SIGKILL
    Terminate the master process immediately. Workers will detect that their
    master is dead and will initiate a warm shutdown.


Worker Process
--------------

SIGTERM
    Finish the running task and exit.

SIGKILL
    Terminate the worker process immediately. Master will detect that worker is
    dead and respawn a new one.
