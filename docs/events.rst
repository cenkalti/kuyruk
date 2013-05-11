.. module:: kuyruk

Events
------

Kuyruk has signalling support via
`Blinker <http://discorporate.us/projects/Blinker/>`_ library.
Not to confuse with OS signals they are called "Events" in Kuyruk.

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


.. automethod:: Kuyruk.before_task
Provides arguments:

* sender: Sender of the event
* task: :class:`Task` instance
* args: Positional arguments of the task
* kwargs: Keyword arguments of the task

.. automethod:: Kuyruk.after_task
.. automethod:: Kuyruk.on_return
.. automethod:: Kuyruk.on_exception
