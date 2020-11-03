# Contributing to AsyncDB

## Preparation

AsyncDB is designed to use last syntax of asyncio-tools, for that reason, you'll need to have at least Python 3.7 available for testing.

You can do this with [pyenv][]:

    $ pyenv install 3.7.4
    $ pyenv local 3.7.4


## Setup

Once cloned, create a clean virtual environment and
install the appropriate tools and dependencies:

    $ cd <path/to/asyncdb>
    $ make venv
    $ source .venv/bin/activate
    $ make setup


## Formatting

asyncDB start using *[black][]* for formatting code.

    $ make format


## Testing

Once you've made changes, you should run unit tests,
validate your type annotations, and ensure your code
meets the appropriate style and linting rules:

    $ make test lint


## Submitting

Before submitting a pull request, please ensure
that you have done the following:

* Documented changes or features in README.md
* Added appropriate license headers to new files
* Written or modified tests for new functionality
* Formatted code following project standards
* Validated code and formatting with `make test lint`

[black]: https://github.com/psf/black
[pyenv]: https://github.com/pyenv/pyenv
