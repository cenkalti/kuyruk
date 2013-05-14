Getting Started
===============

Running a function in background requires only few steps with Kuyruk.
This tutorial assumes that you have a RabbitMQ server running in localhost
with default options.


Installing
----------

Kuyruk is available on PyPI. You can install it via pip.

.. code-block:: bash

   $ pip install kuyruk


Defining Tasks
--------------

Instantiate a :class:`~kuyruk.Kuyruk` object somewhere.
Then just put a :meth:`~kuyruk.Kuyruk.task` decorator on top of your function that you
want to run in background. After decorating, when you call the function it
will send the task to default queue instead of invoking it. Since Kuyruk does
not support a result backend yet you should not be using the return value of
the function.

.. code-block:: python

   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task
   def echo(message):
       print message


For more information on defining tasks see the documentation on
:meth:`~kuyruk.Kuyruk.task` decorator.


Sending the Task to RabbitMQ
----------------------------

Kuyuk requires no change in client code. After wrapping a function with
:meth:`~kuyruk.Kuyruk.task` decorator calling the function as usual will send a
message to the queue instead of running the function.


Running a Worker
----------------

.. code-block:: bash

    $ kuyruk worker

Running the above command is enough for processing the tasks in the
default queue.

For more information about workers see :ref:`workers`.
