venv:
	python3.11 -m venv .venv
	echo 'run `source .venv/bin/activate` to start develop asyncDB'

install:
	pip install -e .

develop:
	pip install -e .[uvloop,all]
	pip install -Ur docs/requirements-dev.txt
	flit install --symlink

release:
	lint test clean
	flit publish

format:
	python -m black asyncdb

lint:
	python -m pylint --rcfile .pylintrc asyncdb/*.py
	python -m pylint --rcfile .pylintrc asyncdb/utils/*.py
	python -m black --check asyncdb

test:
	python -m coverage run -m asyncdb.tests
	python -m coverage report
	python -m mypy asyncdb/*.py

perf:
	python -m unittest -v asyncdb.tests.perf

distclean:
	rm -rf .venv
