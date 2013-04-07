class Config(object):

    DEFAULTS = {
        'KUYRUK_RABBIT_HOST': 'localhost',
        'KUYRUK_RABBIT_PORT': 5672,
        'KUYRUK_RABBIT_USER': 'guest',
        'KUYRUK_RABBIT_PASSWORD': 'guest',
        'KUYRUK_EAGER': False,
        'KUYRUK_MAX_RUN_TIME': None,
        'KUYRUK_MAX_TASKS': None,
        'KUYRUK_MAX_LOAD': None,
        'KUYRUK_WORKERS': {},
    }

    def __init__(self, obj):
        for k, v in self.DEFAULTS.iteritems():
            value = getattr(obj, k, v)
            setattr(self, k[7:], value)
