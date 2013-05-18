Extending
=========

There are two classes you can extend:
:class:`~kuyruk.Task` and :class:`~kuyruk.Worker`.


Extending Task Class
--------------------

:class:`~kuyruk.Kuyruk` object constructor and :meth:`~kuyruk.Kuyruk.task`
decorator accept an optional ``task_class`` argument.
You can pass a subclass of :class:`~kuyruk.Task` to change it's behavior.


Extending Worker Class
----------------------

Worker class can be given as a string as the value of
:attr:`~kuyruk.config.Config.WORKER_CLASS` in the configuration.
When a worker is started, this class will be instantiated and
it's :meth:`~kuyruk.Worker.run` method will be called.
