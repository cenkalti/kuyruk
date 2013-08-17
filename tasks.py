from invoke import run, task


@task
def test():
    run("nosetests")


@task
def bump(part):
    assert part in ('major', 'minor', 'patch')
    run("bumpversion %s" % part)


@task
def uplaod():
    run("python setup.py sdist upload")


@task('test')
def coverage():
    run("coverage combine")
    run("coverage html")
    run("open coverage_html_report/index.html")
