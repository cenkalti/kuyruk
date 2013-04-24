Kuyruk
============

Kuyruk is a simple and easy way of distributing tasks to run on other servers.
It uses `RabbitMQ <http://www.rabbitmq.com>`_ message broker for routing tasks.

.. image:: https://travis-ci.org/cenkalti/kuyruk.png
   :target: https://travis-ci.org/cenkalti/kuyruk

How to install?
---------------

.. code:: bash

   $ pip install kuyruk

How to run tests?
-----------------

.. code:: bash

   $ pip install nose pexpect
   $ nosetests


How to define tasks?
--------------------

.. code:: python

   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task
   def echo(message):
       print message


How to run the worker?
----------------------

.. code:: bash

   $ kuyruk
