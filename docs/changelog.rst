Changelog
=========

Here you can see the full list of changes between each Kuyruk release.

Version 10.0.0
--------------

Released on 05-12-2023.

- ``Kuyruk.connection()`` does not return new connection anymore. It returns the underlying connection. The connection is locked while the context manager is active. If you need to hold the connection for a long time, use ``Kuyruk.new_connection()`` to create a separate connection.

Version 9.4.0
-------------

Relesed on 27-03-2020.

- Fixed various heartbeat related bugs.

Version 9.0.6
-------------

Released on 09-03-2018.

- Removed support for Python 2.
- Workers do not change process title anymore.
- Removed ``reject_delay`` argument from ``Kuyruk.task`` decorator.
- Removed task decorator syntax without arguments.
- Improved heartbeat handling.
- Worker does not retry on ``TimeoutError`` exceptions.

Version 8.5.0
-------------

Released on 25-10-2017.

- Added Windows support.

Version 8.3.0
-------------

Released on 15-04-2017.

- Added "Kuyruk.send_tasks_to_queue" method for batch sending.

Version 8.2.0
-------------

Released on 22-02-2017.

- Added delay for failed and rejected taks.

Version 8.1.0
-------------

Released on 25-11-2016.

- Added RabbitMQ connection timeout values to config.

Version 8.0.0
-------------

Released on 23-11-2016.

- Removed "queue" key from task description dictionary.
- Improve error handling when closing connections and channels.

Version 7.0.0
-------------

Released on 01-11-2016.

- Removed ``local`` argument from ``Kuyruk.task`` decorator.

Version 6.0.0
-------------

Released on 01-11-2016.

- Changed ``WORKER_MAX_LOAD`` behavior.
  ``None`` disables the feature.
  Set to ``-1`` for number of CPUs on host.
- Add argumens to worker command to override ``WORKER_MAX_LOAD`` and
  ``WORKER_MAX_RUN_TIME`` config values.
- Renamed ``ConnectionError`` to ``HeartbeatError``.
- Removed ``Task.run_in_queue`` method.
  Use ``Task.send_to_queue`` with ``wait_result`` argument instead.
- Removed ``local`` argument from ``Task.send_to_queue`` method.
  Pass ``host="localhost"`` for sending to local queue.
- Removed ``local`` argument from worker command.
  If queue name ends with ``".localhost"`` hostname will be appended to queue name.
- Removed deprecated ``Worker.queue`` property.
- Removed ``WORKER_LOGGING_CONFIG`` configuration value.
- Added ``--logging-level`` to worker command arguments.
- Removed ``Worker.config`` property.
- Added ``message_ttl`` argument to ``Task.send_to_queue`` method.

Version 5.1.0
-------------

Released on 26-10-2016.

- Added ``Task.run_in_queue`` context manager for getting task results.

Version 5.0.0
-------------

Released on 19-10-2016.

- Removed ``kuyruk_host`` and ``kuyruk_local`` arguments from task calls.
- Exported ``Task.send_to_queue`` method.

Version 4.1.2
-------------

Released on 18-10-2016.

- Fixed 1 second delay after processing a task.
- Fixed a depreciation warning from recent version of amqp library.

Version 4.1.0
-------------

Released on 11-03-2016.

- Workers can consume tasks from multiple queues.

Version 4.0.7
-------------

Released on 12-02-2016.

- Export Task.name property for fixing a bug in kuyruk-manager.

Version 4.0.6
-------------

Released on 08-02-2016

- Fixed a bug related with Python 2.7.11, uWSGI and setproctitle.

Version 2.0.0
-------------

Released on 03-12-2014.

This is major rewrite of Kuyruk and it is not backwards compatible.

- Added Python 3 support.
- Replaced pika with amqp.
- Fixed multi-threading issues.
- Removed master subcommand.
- Removed scheduler subcommand.
- Removed requeue subcommand.
- Removed manager subcommand.
- Exceptions are not sent to Sentry.
- Failed tasks are not saved to Redis anymore.
- Failed tasks are retried in the same worker.
- Unknown keys in config are now errors.
- Changed some config variable names.
- Worker command takes Kuyruk instance instead of config file.

Version 1.2.1
-------------

Released on 25-08-2014.

- Fixed a worker startup bug happens when running workers as another user.

Version 1.2.0
-------------

Released on 09-06-2014.

- Added periodic task scheduler feature.

Version 1.1.0
-------------

Released on 07-06-2014.

- Added Task.delay() function alias for easy migration from Celery.

Version 1.0.0
-------------

Released on 20-05-2014.

- Use rpyc library for manager communication.

Version 0.24.3
--------------

Released on 05-03-2014.

- Reverted the option to give Task class from configuration. This caused
  master to import from user code.
- Added sleep after respawn_worker to prevent cpu burning.

Version 0.24.2
--------------

Released on 16-01-2014.

- Added the option to give Task class from configuration.

Version 0.24.1
--------------

Released on 13-01-2014.

- Prevented 'close' to be called on a nonexistent connection.

Version 0.23.3
--------------

Released on 15-09-2013.

- Fix the bug about freezing processes on exit.

Version 0.23.2
--------------

Released on 12-09-2013.

- Fix unclosed socket error on manager.

Version 0.23.0
--------------

Released on 30-08-2013.

- Removed InvalidCall exception type. TypeError or AttributeError is raised
  instead.
- If a kuyruk process exits with a signal, the exit code will be 0.

Version 0.22.1
--------------

Released on 27-08-2013.

- Master uses os.wait() instead of polling workers every second.

Version 0.22.0
--------------

Released on 25-08-2013.

- Use forking again instead Popen after fixing import issue.
- Add "Quit Task" button to Manager interface.

Version 0.21.0
--------------

Released on 17-08-2013.

- Drop support for Python 2.6.
- Switch back to subprocess module from forking.

Version 0.20.3
--------------

Released on 10-08-2013.

- Use fork() directly instead of subprocess.Popen() when starting workers
  from master.

Version 0.20.2
--------------

Released on 03-08-2013.

First public release.
