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

    @kuyruk.on_prerun
    def new_session(task, args, kwargs):
        session.remove()

    @kuyruk.on_postrun
    def commit_session(task, args, kwargs):
        session.commit()


.. automethod:: Kuyruk.on_prerun
Provides arguments:

* sender: Sender of the event
* task: :class:`Task` instance
* args: Positional arguments of the task
* kwargs: Keyword arguments of the task

.. automethod:: Kuyruk.on_postrun
.. automethod:: Kuyruk.on_success
.. automethod:: Kuyruk.on_failure
