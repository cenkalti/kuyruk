upload:
	python setup.py sdist upload

coverage:
	pytest
	coverage combine
	coverage html
	open coverage_html_report/index.html
