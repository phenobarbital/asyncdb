.venv:
	python3.8 -m venv .venv
	source .venv/bin/activate && make setup dev
	echo 'run `source .venv/bin/activate` to start develop asyncDB'

venv: .venv

setup:
	python -m pip install -Ur docs/requirements.txt

dev:
	flit install --symlink

release: lint test clean
	flit publish

format:
	python -m black asyncdb

lint:
	python -m pylint --rcfile .pylint asyncdb/*.py
	python -m black --check asyncdb

test:
	python -m coverage run -m asyncdb.tests
	python -m coverage report
	python -m mypy asyncdb/*.py

perf:
	python -m unittest -v asyncdb.tests.perf

distclean: clean
	rm -rf .venv
