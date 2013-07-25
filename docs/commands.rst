.. _commands:

Commands
========

Kuyruk has only one command which consists of several sub-commands.
``kuyruk`` command usually takes a single ``--config`` argument
which is the ``.py`` file containing configuration options.

If no ``--config`` option is given it will be looked from environmental
variable ``KUYRUK_CONFIG``. Set it with::

    $ export KUYRUK_CONFIG=/home/cenk/app1/config/kuyruk.py

... or add it to your ``.bashrc`` file.

Each option in config can be overriden from command line. Example::

    $ kuyruk --config config.py --max-run-time 60 SUBCOMMAND

Please run ``kuyruk --help`` to see other options.


Worker
------

``worker`` sub-command starts a worker that consumes messages from a queue and
run tasks. Example::

    $ kuyruk --config config.py worker --queue mp4_conversion

If called with no parameters it will use default configuration options and
queue. Example::

    $ kuyruk worker

``worker`` sub-command only starts one process and does not do any
multiprocessing. For starting multiple workers we have a ``master`` sub-command.


Master
------

``master`` is the sub-command for starting multiple workers. It reads the list
of queues from configuration and start a worker for each queue.
After starting workers, master checks them periodically to see if they are alive.
If any worker dies unexpectedly, master starts a new worker to replace dead one.
Master's only job is supervising workers.
Master does not connect to RabbitMQ directly, each worker maintains its own
connection so there is nothing shared between workers. Usage::

    $ kuyruk --config config.py master

Also workers poll the master see if it is alive.
If the master dies, workers are shut down gracefully.


Requeue
-------

If the configuration option :attr:`~kuyruk.config.Config.SAVE_FAILED_TASKS`
is enabled failed tasks are saved to another queue (``kuyruk_failed``).
After the task is fixed you can re-queue this tasks by running the ``requeue``
subcommand. Usage::

    $ kuyruk --config config.py requeue


Manager
-------

Kuyruk has a simple management interface for giving visibility accross your
workers and queues. It also has ability to send simple actions to your workers
such as shutting down or killing workers. Do not forget to set
:attr:`~kuyruk.config.Config.MANAGER_HOST` configuration value to make your
workers connect to manager.

Usage::

    $ kuyruk --config config.py manager

After running the above command on your localhost you can browse it from
http://localhost:16500.
