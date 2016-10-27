upload:
	python setup.py sdist upload

coverage:
	nosetests --with-coverage --cover-package kuyruk --cover-erase
	coverage combine
	coverage html
	open coverage_html_report/index.html
