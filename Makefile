.PHONY: docs tests
init:
	pip install pipenv --upgrade
	pipenv install --skip-lock
	pipenv run pip freeze

developer:
	pipenv install --dev --skip-lock

tests:
	pipenv run tox

publish:
	pipenv run python setup.py sdist bdist_wheel
	pipenv run twine upload dist/*
	rm -fr build dist .egg s3path.egg-info

check: tests
