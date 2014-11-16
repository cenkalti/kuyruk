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
