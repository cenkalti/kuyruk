Changelog
=========

Here you can see the full list of changes between each Kuyruk release.

Version 2.0.0
-------------

This is major rewrite of Kuyruk and it is not backwards compatible.

- Add Python 3 support.
- Replaced pika with amqp.
- Fixed multi-threading issues.
- Removed master subcommand.
- Removed scheduler subcommand.
- Removed requeue subcommand.
- Removed manager subcommand.
- Exceptions are not sent to Sentry.
- Failed tasks are not saved to redis anymore.
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
