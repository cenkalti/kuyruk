Signals
=======

The following sections describe that how will Kuyruk processes react to signals.


Common Signals
--------------

SIGINT
    If Kuyruk is run from an interactive shell the first signal initiates a
    warm shutdown. Second signal does a cold shutdown.

    If not run from an interactive shell, it is the same as SIGQUIT.

SIGUSR1
    Print stacktrace. Useful for debugging stuck processes.


Master Specific Signlas
-----------------------

SIGTERM
    Warm shutdown: Shutdown workers gracefully and exit.

SIGQUIT
    Cold shutdown: Kill workers and exit.

SIGABRT
    Exit immediately without stopping workers. Workers will become orphan and
    initiate a warm shutdown after detecting that their parent has exited.

SIGKILL
    Terminate the master process immediately. Workers will detect that their
    master is dead and will initiate a warm shutdown.


Worker Specific Signals
-----------------------

SIGTERM
    Finish the running task and exit. Warm shutdown in other words.

SIGKILL
    Terminate the worker process immediately. Master will detect that worker is
    dead and respawn a new one.
