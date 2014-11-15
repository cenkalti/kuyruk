from __future__ import absolute_import

import json
from datetime import datetime
from functools import partial

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime(DATETIME_FORMAT)
        else:
            return super(JSONEncoder, self).default(obj)


class JSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, *args, object_hook=self.object_hook,
                                  **kwargs)

    @staticmethod
    def object_hook(obj):
        if isinstance(obj, dict):
            for key in obj:
                if not isinstance(obj[key], basestring):
                    continue
                try:
                    obj[key] = datetime.strptime(obj[key], DATETIME_FORMAT)
                except ValueError:
                    pass

        return obj

dumps = partial(json.dumps, cls=JSONEncoder)
loads = partial(json.loads, cls=JSONDecoder)

if __name__ == "__main__":
    print dumps({"date": datetime.now()})
    print loads(dumps({"date": datetime.now()}))
