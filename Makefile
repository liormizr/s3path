.PHONY: docs tests
init:
	pip install pipenv --upgrade
	pipenv install
	pipenv run pip freeze

developer:
	pipenv install --dev

tests:
	pipenv run tox

check: tests
