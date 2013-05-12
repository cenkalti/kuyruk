Kuyruk
======

Kuyruk is a simple and easy way of distributing tasks to run on servers.

It uses `RabbitMQ <http://www.rabbitmq.com>`_ as message broker and depends on
`Pika <http://pika.readthedocs.org/en/latest/>`_
which is a pure-Python RabbitMQ client library.

.. image:: https://travis-ci.org/cenkalti/kuyruk.png
   :target: https://travis-ci.org/cenkalti/kuyruk


How to install?
---------------

Kuyruk is available on PyPI.

.. code-block:: bash

   $ pip install kuyruk


How to run tests?
-----------------

Normally you don't need this but it is easy. Make sure that RabbitMQ
is running before running tests.

.. code-block:: bash

   $ git clone git://github.com/cenkalti/kuyruk.git
   $ cd kuyruk
   $ pip install -r kuyruk/test/requirements.txt
   $ nosetests


How to define tasks?
--------------------

Instantiate a ``Kuyruk`` object somewhere.
Then just put a ``kuyruk.task`` decorator on top of your function that you
want to run in background. After decorating, when you call the function it
will send the task to default queue instead of running the function.
Since Kuyruk does not support a result backend yet you should not be
using the return value of the function.

.. code-block:: python

   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task
   def echo(message):
       print message

   # This will send a message to queue
   echo('Hello, Kuyruk.')


How to run the worker?
----------------------

Running the binary ``kuyruk`` is enough for processing the tasks in the
default queue.

.. code-block:: bash

   $ kuyruk


Where is the documentation?
---------------------------
Here it is: http://kuyruk.readthedocs.org/en/latest/
