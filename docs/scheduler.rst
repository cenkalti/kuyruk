Scheduler
=========

Scheduler is a basic process scheduler, that kicks off tasks at regular intervals

To run the scheduler, you need to have a config like this:

.. code-block:: python

    SCHEDULE = {
        'runs-every-10-seconds':{
            'task': 'tasks.echo',
            'schedule': timedelta(seconds=10)
        },
        'runs-every-10-minutes':{
            'task': 'tasks.foo',
            'schedule': timedelta(minutes=10),
            'args': ['bar', 'baz']
        }
    }

then you can run scheduler with


.. code-block:: bash

    kuyruk --config=tasks.py --logging-level=debug scheduler

It saves the last runing times to a shelve file database, so it won't start all the jobs if you restart. The
location of the database can be altered by

.. code-block:: python

    SCHEDULER_FILE_NAME = '/home/users/ybrs/scheduler'
