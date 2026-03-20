#!/usr/bin/env python
"""Minimal setup.py for Cython extensions only."""

from Cython.Build import cythonize
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext

COMPILE_ARGS = ["-O3"]

extensions = [
    Extension(
        name='asyncdb.exceptions.exceptions',
        sources=['asyncdb/exceptions/exceptions.pyx'],
        extra_compile_args=COMPILE_ARGS,
        language="c"
    ),
    Extension(
        name='asyncdb.utils.types',
        sources=['asyncdb/utils/types.pyx'],
        extra_compile_args=COMPILE_ARGS,
        language="c++"
    ),
]

class BuildExtensions(build_ext):
    """Custom build_ext command to ensure cythonization during the build step."""
    def build_extensions(self):
        try:
            self.extensions = cythonize(self.extensions)
        except ImportError:
            print("Cython not found. Extensions will be compiled without cythonization!")
        super().build_extensions()

if __name__ == "__main__":
    setup(
        ext_modules=cythonize(extensions),
        cmdclass={"build_ext": BuildExtensions},
    )
