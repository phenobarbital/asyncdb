# Contributing to AsyncDB

## Preparation

AsyncDB is designed to use last syntax of asyncio-tools, for that reason, you'll need to have at least Python 3.8 available for testing.

You can do this with [pyenv][]:


    $ pyenv install 3.8.1
    $ pyenv local 3.8.1

Or using virtualenv:

    $ python3.8 -m venv .venv

Also, we can use the command "make venv" inside of Makefile.

    $ make venv

## Setup

Once cloned, create a clean virtual environment and
install the appropriate tools and dependencies:

    $ cd <path/to/asyncdb>
    $ make venv
    $ source .venv/bin/activate
    $ make setup


## Formatting

asyncDB start using *[black][]* for formating code.

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
