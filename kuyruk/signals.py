from blinker import Signal

task_init = Signal()
task_prerun = Signal()
task_postrun = Signal()
task_success = Signal()
task_error = Signal()
task_failure = Signal()
task_presend = Signal()
task_postsend = Signal()
worker_failure = Signal()
