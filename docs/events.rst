Events
------

Kuyruk has signalling support via
`Blinker <http://discorporate.us/projects/Blinker/>`_ library.

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

TODO document arguments to signals.