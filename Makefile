# AsyncDB Makefile
# This Makefile provides a set of commands to manage the AsyncDB project.

.PHONY: venv install develop setup dev release format lint test clean distclean lock sync

# Python version to use
PYTHON_VERSION := 3.11

# Auto-detect available tools
HAS_UV := $(shell command -v uv 2> /dev/null)
HAS_PIP := $(shell command -v pip 2> /dev/null)

# Install uv for faster workflows
install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "uv installed! You may need to restart your shell or run 'source ~/.bashrc'"
	@echo "Then re-run make commands to use faster uv workflows"

# Create virtual environment
venv:
	uv venv --python $(PYTHON_VERSION) .venv
	@echo 'run `source .venv/bin/activate` to start develop with AsyncDB'

# Install production dependencies using lock file
install:
	uv sync --frozen --no-dev --extra all
	@echo "Production dependencies installed. Use 'make develop' for development setup."

# Generate lock files (uv only)
lock:
ifdef HAS_UV
	uv lock
else
	@echo "Lock files require uv. Install with: pip install uv"
endif

# Install all dependencies including dev dependencies
develop:
	uv sync --frozen --extra all --extra dev

# Alternative: install without lock file (faster for development)
develop-fast:
	uv pip install -e .[all,dev]

# Setup development environment from requirements file (if you still have one)
setup:
	uv pip install -r docs/requirements-dev.txt

# Install in development mode
dev:
	uv pip install -e .

# Build and publish release
release: lint test clean
	uv build
	uv publish

# Format code
format:
	uv run black asyncdb

# Lint code
lint:
	uv run pylint --rcfile .pylintrc asyncdb/*.py
	uv run black --check asyncdb

# Run tests with coverage
test:
	uv run coverage run -m asyncdb.tests
	uv run coverage report
	uv run mypy asyncdb/*.py

# Alternative test command using pytest directly
test-pytest:
	uv run pytest

# Add new dependency and update lock file
add:
	@if [ -z "$(pkg)" ]; then echo "Usage: make add pkg=package-name"; exit 1; fi
	uv add $(pkg)

# Add development dependency
add-dev:
	@if [ -z "$(pkg)" ]; then echo "Usage: make add-dev pkg=package-name"; exit 1; fi
	uv add --dev $(pkg)

# Remove dependency
remove:
	@if [ -z "$(pkg)" ]; then echo "Usage: make remove pkg=package-name"; exit 1; fi
	uv remove $(pkg)

# Compile Cython extensions using setup.py
build-cython:
	@echo "Compiling Cython extensions..."
	python setup.py build_ext

# Build Cython extensions in place (for development)
build-inplace:
	@echo "Building Cython extensions in place..."
	python setup.py build_ext --inplace

# Full build using uv
build: clean
	@echo "Building package with uv..."
	uv build

# Update all dependencies
update:
	uv lock --upgrade

# Show project info
info:
	uv tree

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -name "*.so" -delete
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete."

# Remove virtual environment
distclean:
	rm -rf .venv
	rm -rf uv.lock

help:
	@echo "Available targets:"
	@echo "  venv              - Create virtual environment"
	@echo "  install           - Install production dependencies"
	@echo "  develop           - Install development dependencies"
	@echo "  build             - Build package"
	@echo "  release           - Build and publish package"
	@echo "  test              - Run tests"
	@echo "  format            - Format code"
	@echo "  lint              - Lint code"
	@echo "  clean             - Clean build artifacts"
	@echo "  install-uv        - Install uv for faster workflows"
	@echo "Current setup: Python $(PYTHON_VERSION)"
