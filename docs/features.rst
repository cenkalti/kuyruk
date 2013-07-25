Features
========


Simple
------
Simple means *not complex*. All design decisions is based on simplicity.
Speed is not first priority.

Kuyruk only supports RabbitMQ and will not support another message broker ever.
For this reason Kuyruk tries to utilize RabbitMQ and AMQP as much as possible.


Easy
----
Kuyruk requires no change in client code other than adding a decorator on top
of a function in order to convert it to a "task".
Also, sensible defaults have been set for configuration options and
can be run without configuring while developing your application.


Robust
------
Kuyruk is designed for long running mission critical jobs. For this reason
Kuyruk sends acknowledgement only after the task is fully processed.
