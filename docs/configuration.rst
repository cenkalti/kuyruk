.. module:: kuyruk.config

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

.. autoattribute:: Config.IMPORT_PATH
.. autoattribute:: Config.IMPORTS
.. autoattribute:: Config.EAGER
.. autoattribute:: Config.MAX_LOAD
.. autoattribute:: Config.MAX_RUN_TIME
.. autoattribute:: Config.SAVE_FAILED_TASKS
.. autoattribute:: Config.WORKERS


Connection Settings
-------------------------

.. autoattribute:: Config.RABBIT_HOST
.. autoattribute:: Config.RABBIT_PORT
.. autoattribute:: Config.RABBIT_USER
.. autoattribute:: Config.RABBIT_PASSWORD


Manager Settings
-------------------------

.. autoattribute:: Config.MANAGER_HOST
.. autoattribute:: Config.MANAGER_PORT
.. autoattribute:: Config.MANAGER_HTTP_PORT
