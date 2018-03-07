Signals
=======

Kuyruk can be extended via signals. Kuyruk has a signalling support via
`Blinker <http://pythonhosted.org/blinker/>`_ library.


Example
-------

Here is an example for clearing the SQLAlchemy's scoped_session before
executing the function and commiting it after the task is executed:

.. code-block:: python

    from kuyruk import Kuyruk
    from kuyruk.signals import task_prerun, task_postrun

    from myapp.orm import Session

    kuyruk = Kuyruk()

    @task_prerun.connect_via(kuyruk)
    def open_session(sender, task=None, **extra):
        task.session = Session()

    @task_postrun.connect_via(kuyruk)
    def close_session(sender, task=None, **extra):
        task.session.close()

    @kuyruk.task()
    def task_with_a_session():
        session = task_with_a_session.session
        # Work with the session...
        session.commit()

List of Signals
---------------

.. automodule:: kuyruk.signals
   :member-order: bysource
   :members:
