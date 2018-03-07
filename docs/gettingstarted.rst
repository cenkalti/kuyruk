Getting Started
===============

Running a function in background requires only few steps with Kuyruk.
This tutorial assumes that you have a running RabbitMQ server on localhost
with default configuration.

Following files and commands are in
`example directory <https://github.com/cenkalti/kuyruk/tree/master/example>`_
for convenience.


Installing
----------

Kuyruk is available on PyPI. You can install it via pip.

.. code-block:: bash

   $ pip install kuyruk


Defining Tasks
--------------

Instantiate a :class:`~kuyruk.Kuyruk` object and put a
:meth:`~kuyruk.Kuyruk.task` decorator on top of your function.
This will convert your function into a :class:`~kuyruk.Task` object.

.. code-block:: python

   # tasks.py
   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task()
   def echo(message):
       print message


You can specify some options when defining task. See
:meth:`~kuyruk.Kuyruk.task` for details.


Sending the Task to RabbitMQ
----------------------------

When you call the :class:`~kuyruk.Task` object, Kuyruk will serialize the task
as JSON and will send it to a queue on RabbitMQ instead of running it.

.. code-block:: python

   import tasks
   tasks.echo("Hello, World!")


Running a Worker
----------------

Run the following command to process tasks in default queue.

.. code-block:: bash

    $ kuyruk --app tasks.kuyruk worker
