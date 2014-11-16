Extending
=========

There are two classes you can extend:
:class:`~kuyruk.Task` and :class:`~kuyruk.Worker`.


Extending Task Class
--------------------

:class:`~kuyruk.Kuyruk` object constructor and :meth:`~kuyruk.Kuyruk.task`
decorator accept an optional ``task_class`` argument.
You can pass a subclass of :class:`~kuyruk.Task` to change it's behavior.

Example:

.. code-block:: python

    from kuyruk import Kuyruk, Task
    from kuyruk.events import task_prerun, task_postrun
    from myapp.orm import Session

    kuyruk = Kuyruk()

    class DatabaseTask(Task):
        """Open a new database session before running tasks
        and close on finish."""

        def setup(self):
            self.connect_signal(task_prerun, self.open_session)
            self.connect_signal(task_postrun, self.close_session)

        def open_session(self, *args, **kwargs):
            self.session = Session()

        def close_session(self, *args, **kwargs):
            self.session.close()

    @kuyruk.task(task_class=DatabaseTask)
    def a_task_with_a_session(some_arg):
        session = self.session
        # Work with the session...
        session.commit()


Extending Worker Class
----------------------

Worker class can be given as a string as the value of
:attr:`~kuyruk.Config.WORKER_CLASS` in the configuration.
When a worker is started, this class will be instantiated and
it's :meth:`~kuyruk.Worker.run` method will be called.
