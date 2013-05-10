from __future__ import absolute_import

import json
import datetime

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime(DATETIME_FORMAT)
        else:
            return super(JSONEncoder, self).default(obj)


class JSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, *args,
                                  object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj):
        if isinstance(obj, dict):
            for key in obj:
                if not isinstance(obj[key], basestring):
                    continue
                try:
                    obj[key] = datetime.datetime.strptime(obj[key],
                                                          DATETIME_FORMAT)
                except ValueError:
                    pass

        return obj


if __name__ == "__main__":
    encoder = JSONEncoder()
    print encoder.encode({"date": datetime.datetime.now()})

    decoder = JSONDecoder()
    print decoder.decode(encoder.encode({"date": datetime.datetime.now()}))
