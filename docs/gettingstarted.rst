Getting Started
===============

Running a function in background requires only few steps with Kuyruk.
This tutorial assumes that you have a RabbitMQ server running at localhost
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

Instantiate a :class:`~kuyruk.Kuyruk` object somewhere.
Then just put a :meth:`~kuyruk.Kuyruk.task` decorator on top of your function
that you want to run in background. After decorating, when you call the
function it will send the task to default queue instead of invoking it.

.. code-block:: python

   # tasks.py
   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task
   def echo(message):
       print message


You can specify some options when defining task. See
:meth:`~kuyruk.Kuyruk.task` for details.


Sending the Task to RabbitMQ
----------------------------

Kuyuk requires no change in client code. After wrapping a function with
:meth:`~kuyruk.Kuyruk.task` decorator calling the function as usual will send a
message to the queue instead of running the function.


Running a Worker
----------------

.. code-block:: bash

    $ kuyruk --app tasks.kuyruk worker

Running the above command is enough for processing the tasks in the
default queue.
