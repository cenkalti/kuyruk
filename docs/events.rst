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


These decorators below are available from both :class:`Kuyruk` and :class:`Task`
instances.

.. automethod:: kuyruk.Kuyruk.on_prerun
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task

.. automethod:: kuyruk.Kuyruk.on_postrun
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task

.. automethod:: kuyruk.Kuyruk.on_success
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task

.. automethod:: kuyruk.Kuyruk.on_failure
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task
    * exc_info: Exception info returned from ``sys.exc_info()``

.. automethod:: kuyruk.Kuyruk.on_presend
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task

.. automethod:: kuyruk.Kuyruk.on_postsend
    :noindex:

    Provides arguments:

    * sender: Sender of the event
    * task: :class:`Task` instance
    * args: Positional arguments of the task
    * kwargs: Keyword arguments of the task
