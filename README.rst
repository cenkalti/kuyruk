============
Kuyruk
============

Distributed task queue.

.. image:: https://travis-ci.org/cenkalti/kuyruk.png
   :target: https://travis-ci.org/cenkalti/kuyruk

How to install?
===============

::

$ pip install -e git+https://github.com/cenkalti/kuyruk.git#egg=kuyruk

How to run tests?
=================

::

$ pip install nose pexpect
$ nosetests


How to define tasks?
====================

::

   from kuyruk import Kuyruk

   kuyruk = Kuyruk()

   @kuyruk.task
   def echo(message):
       print message


How to run the worker?
======================

::

$ kuyruk
