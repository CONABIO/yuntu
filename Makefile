TEST_DIR := tests/
DOCS_SOURCE_DIR := docs/source/
DOCS_BUILD_DIR := docs/build/

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

lint:
	poetry run pylint --exclude=.tox

test:
	poetry run pytest --verbose --color=yes $(TEST_DIR)

tox:
	poetry run tox

clean-docs:
	rm -rf $(DOCS_BUILD_DIR)

docs: clean-docs
	poetry run sphinx-build -b doctest $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)
	poetry run sphinx-build -b html $(DOCS_SOURCE_DIR) $(DOCS_BUILD_DIR)

serve-coverage:
	python3 -m http.server --directory htmlcov/ 8080

serve-docs:
	python3 -m http.server --directory $(DOCS_BUILD_DIR) 9090
