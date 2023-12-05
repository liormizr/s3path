.PHONY: docs tests
init:
	python -m pip install --upgrade pip
	python -m pip install --upgrade pipenv
	pipenv install --skip-lock
	pipenv run pip freeze

developer:
	pipenv install --dev --skip-lock

tests:
	pipenv run pytest

publish:
	pipenv run python setup.py sdist bdist_wheel
	pipenv run twine upload dist/*
	rm -fr build dist .egg s3path.egg-info

check: tests
