from blinker import Namespace

_signals = Namespace()

before_task = _signals.signal('before-task')
after_task = _signals.signal('after-task')
on_exception = _signals.signal('on-exception')
on_return = _signals.signal('on-return')
