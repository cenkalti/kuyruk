Kuyruk: Simple task queue
=========================

Welcome to Kuyruk’s documentation.
This documentation covers the usage of the library
and describes how to run workers.


About Kuyruk
------------

Kuyruk is a simple and easy way of distributing tasks to run on servers.

It uses `RabbitMQ <http://www.rabbitmq.com>`_ as message broker and depends on
`amqp <http://amqp.readthedocs.org/>`_
which is a pure-Python RabbitMQ client library.
Compatible with Python 3.10+.

All design decisions is based on simplicity.
Speed is not first priority. Kuyruk only supports RabbitMQ and does not plan
to support any other backend.

Kuyruk requires no change in client code other than adding a decorator on top
of a function in order to convert it to a :class:`~kuyruk.Task`.
Also, sensible defaults have been set for configuration options and
can be run without configuring during the development of your application.

Kuyruk is designed for long running mission critical jobs. For this reason
Kuyruk sends acknowledgement only after the task is fully processed.


User's Guide
------------

.. toctree::
   :maxdepth: 1

   gettingstarted
   worker
   signals
   extensions
   changelog


API Reference
-------------

.. toctree::
   :maxdepth: 2

   api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
