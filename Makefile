TEST_DIR := tests/

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

coverage:
	poetry run coverage run -m pytest $(TEST_DIR)
	poetry run coverage report -m
	poetry run coverage html
	python3 -m http.server --directory htmlcov/ 8080

lint:
	poetry run pylint --exclude=.tox

test:
	pytest --verbose --color=yes $(TEST_DIR)

clean-docs:
	rm -rf docs/build/

build-docs: clean-docs
	poetry run sphinx-build docs/source/ docs/build/

serve-docs:
	python3 -m http.server --directory docs/build/ 9090

docs: build-docs serve-docs
