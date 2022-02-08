venv:
	python3.9 -m venv .venv
	echo 'run `source .venv/bin/activate` to start develop asyncDB'

develop:
	pip install wheel==0.37.0
	pip install -e .
	python -m pip install -Ur docs/requirements.txt

dev:
	flit install --symlink

release: lint test clean
	flit publish

format:
	python -m black asyncdb

lint:
	python -m pylint --rcfile .pylint asyncdb/*.py
	python -m pylint --rcfile .pylint asyncdb/utils/*.py
	python -m black --check asyncdb

setup_test:
	pip install pytest>=6.0.0
	pip install pytest-asyncio==0.18.0
	pip install pytest-xdist==2.1.0
	pip install pytest-assume==2.4.2

test:
	python -m coverage run -m asyncdb.tests
	python -m coverage report
	python -m mypy asyncdb/*.py

perf:
	python -m unittest -v asyncdb.tests.perf

distclean:
	rm -rf .venv
