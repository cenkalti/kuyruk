Workers
=======

``kuyruk`` is the name of the command that is used to run workers.
A worker consists of two types of processes: Master process and worker process.
As the name implies, master is the controlling process of workers.
When you run the ``kuyruk`` command it will spawn a worker process for each
queue.
``kuyruk`` takes and ``--config`` option for configuration options.
If it is not passed, it uses the default values.

Example:

.. code-block:: bash

    $ kuyruk --config=config.py

You can override most configuration options from command line.
Run ``kuyruk --help`` to see more options.

After reading the list of queues from configuration, the master spawns a worker
for each queue.
Then, each worker tries to connect to RabbitMQ.
Each worker maintains its own connection so there is nothing shared between
workers.
Also, master does not connect to RabbitMQ. It's only job is supervising workers.

When a worker dies, master detects it and spawn another worker to replace the
old one.
A worker can die if anything unexpected happens such as connection disconnect.
Also workers poll the master see if it is alive and if master dies, each
worker initiates a graceful shutdown.
