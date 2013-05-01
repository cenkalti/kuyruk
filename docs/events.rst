Events
------

If a function is registered as
event handler it will be executed when the event is triggered.
In order to register a function as event handler you need to wrap your function
with a decorator.

Here is the example for clearing the SQLAlchemy's scoped_session before
executing the function and commiting it after the task is executed:

.. code-block:: python

    @kuyruk.before_task
    def new_session(task, args, kwargs):
        session.remove()

    @kuyruk.after_task
    def commit_session(task, args, kwargs):
        session.commit()


.. method:: before_task(task, args, kwargs)

    Handler called before the task function is executed.

    :param task: :class:`~kuyruk.task.Task` instance that is going to be executed.
    :param args: Original arguments for the task.
    :param kwargs: Original keyword arguments for the task.

    The return value of this handler is ignored.


.. method:: after_task(task, args, kwargs)

    Handler called after the task function is executed.

    :param task: :class:`~kuyruk.task.Task` instance that is going to be executed.
    :param args: Original arguments for the task.
    :param kwargs: Original keyword arguments for the task.

    The return value of this handler is ignored.

