class Config(object):
    """Kuyruk configuration object"""

    # Defaults
    RABBIT_HOST = 'localhost'
    RABBIT_PORT = 5672
    RABBIT_USER = 'guest'
    RABBIT_PASSWORD = 'guest'
    EAGER = False
    MAX_LOAD = None
    SAVE_FAILED_TASKS = False
    WORKERS = {}

    def __init__(self, obj):
        """Populate from obj.
        If the key is not found in obj, default is used.
        "KUYRUK_" prefix is stripped in attributes.

        """
        if not isinstance(obj, dict):
            obj = obj.__dict__

        for k, v in obj.iteritems():
            if k.startswith('KUYRUK_'):
                setattr(self, k[7:], v)
