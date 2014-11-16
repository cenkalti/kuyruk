Philosophy
==========


Simple
------
Simple means *not complex*. All design decisions is based on simplicity.
Speed is not first priority. Kuyruk only supports RabbitMQ and does not plan
to support any other backend.


Easy
----
Kuyruk requires no change in client code other than adding a decorator on top
of a function in order to convert it to a :class:`~kuyruk.Task`.
Also, sensible defaults have been set for configuration options and
can be run without configuring during the development of your application.


Robust
------
Kuyruk is designed for long running mission critical jobs. For this reason
Kuyruk sends acknowledgement only after the task is fully processed.
