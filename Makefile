.PHONY: docs tests
init:
	pip install pipenv --upgrade
	pipenv install --dev --skip-lock
	pipenv run pip freeze

tests:
	pipenv run tox

docs:
	pipenv run $(MAKE) -C docs html

check: tests
