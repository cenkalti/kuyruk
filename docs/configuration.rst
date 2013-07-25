.. _configuration-options:

Configuration
=============

Configuration options should be put in a Python module. Pass this module
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

See the :class:`~kuyruk.Config` class for configuration options and
default values.
