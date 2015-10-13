"""
NOTE: These are Invoke tasks, NOT Kuyruk tasks.

Invoke provides a clean, high level API for running shell commands
and defining/organizing task functions from a tasks.py file.

http://www.pyinvoke.org

"""
from invoke import run, task


@task
def bump(part):
    assert part in ('major', 'minor', 'patch')
    run("bumpversion %s" % part)


@task
def upload():
    run("python setup.py sdist upload")


@task
def coverage():
    run("nosetests")
    run("coverage combine")
    run("coverage html")
    run("open coverage_html_report/index.html")
