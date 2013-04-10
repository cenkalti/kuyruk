# Kuyruk [![Build Status](https://travis-ci.org/cenkalti/kuyruk.png)](https://travis-ci.org/cenkalti/kuyruk)

With less magic, better than Celery.

## How do I install?
```bash
$ pip install -e git+https://github.com/cenkalti/kuyruk.git#egg=kuyruk
```

## How to run the tests?
```bash
$ pip install nose pexpect
$ nosetests
```

## How to define tasks?
```python
from kuyruk import Kuyruk

kuyruk = Kuyruk()

@kuyruk.task
def echo(message):
    print message
```

## How to run the worker?
```bash
$ kuyruk
```
