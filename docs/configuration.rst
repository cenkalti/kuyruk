.. _configuration-options:

Configuration
=============

Configuraion options should be put in a Python module. Pass this module
to the :class:`~kuyruk.Kuyruk` instance to configure it.

Example:

.. code-block:: python

    # config.py
    RABBIT_HOST = 'localhost'
    RABBIT_PORT = 5672
    RABBIT_USER = 'guest'
    RABBIT_PASSWORD = 'guest'
    SAVE_FAILED_TASKS = True
    MAX_LOAD = 20


.. code-block:: python

    # tasks.py
    from kuyruk import Kuyruk
    import config

    kuyruk = Kuyruk(config)


Worker Settings
-------------------------

.. autoattribute:: kuyruk.config.Config.WORKER_CLASS
.. autoattribute:: kuyruk.config.Config.IMPORT_PATH
.. autoattribute:: kuyruk.config.Config.IMPORTS
.. autoattribute:: kuyruk.config.Config.EAGER
.. autoattribute:: kuyruk.config.Config.MAX_LOAD
.. autoattribute:: kuyruk.config.Config.MAX_WORKER_RUN_TIME
.. autoattribute:: kuyruk.config.Config.MAX_TASK_RUN_TIME
.. autoattribute:: kuyruk.config.Config.SAVE_FAILED_TASKS
.. autoattribute:: kuyruk.config.Config.QUEUES
.. autoattribute:: kuyruk.config.Config.LOGGING_CONFIG
.. autoattribute:: kuyruk.config.Config.LOGGING_LEVEL
.. autoattribute:: kuyruk.config.Config.SENTRY_DSN


Connection Settings
-------------------------

.. autoattribute:: kuyruk.config.Config.RABBIT_HOST
.. autoattribute:: kuyruk.config.Config.RABBIT_PORT
.. autoattribute:: kuyruk.config.Config.RABBIT_VIRTUAL_HOST
.. autoattribute:: kuyruk.config.Config.RABBIT_USER
.. autoattribute:: kuyruk.config.Config.RABBIT_PASSWORD


Manager Settings
-------------------------

.. autoattribute:: kuyruk.config.Config.MANAGER_HOST
.. autoattribute:: kuyruk.config.Config.MANAGER_PORT
.. autoattribute:: kuyruk.config.Config.MANAGER_HTTP_PORT
