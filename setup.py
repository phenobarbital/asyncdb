#!/usr/bin/env python
"""AsyncDB.

    Asynchronous library for data source connections, used by Navigator.
See:
https://github.com/phenobarbital/asyncdb
"""
import ast
from os import path

from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup


def get_path(filename):
    return path.join(path.dirname(path.abspath(__file__)), filename)


def readme():
    with open(get_path('README.md'), encoding='utf-8') as rd:
        return rd.read()


version = get_path('asyncdb/version.py')
with open(version, 'r', encoding='utf-8') as meta:
    # exec(meta.read())
    t = compile(meta.read(), version, 'exec', ast.PyCF_ONLY_AST)
    for node in (n for n in t.body if isinstance(n, ast.Assign)):
        if len(node.targets) == 1:
            name = node.targets[0]
            if isinstance(name, ast.Name) and \
                    name.id in {
                        '__version__',
                        '__title__',
                        '__description__',
                        '__author__',
                        '__license__', '__author_email__'}:
                v = node.value
                if name.id == '__version__':
                    __version__ = v.s
                if name.id == '__title__':
                    __title__ = v.s
                if name.id == '__description__':
                    __description__ = v.s
                if name.id == '__license__':
                    __license__ = v.s
                if name.id == '__author__':
                    __author__ = v.s
                if name.id == '__author_email__':
                    __author_email__ = v.s

COMPILE_ARGS = ["-O2"]

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
    )
]

setup(
    name="asyncdb",
    version=__version__,
    python_requires=">=3.8.0",
    url="https://github.com/phenobarbital/asyncdb",
    description=__description__,
    keywords=['asyncio', 'asyncpg', 'aioredis', 'aiomcache', 'cassandra'],
    platforms=['POSIX'],
    long_description=readme(),
    long_description_content_type='text/markdown',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: POSIX :: Linux",
        "Environment :: Web Environment",
        "License :: OSI Approved :: BSD License",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
        "Framework :: AsyncIO",
    ],
    author=__author__,
    author_email=__author_email__,
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    package_data={"asyncdb": ["py.typed"]},
    license=__license__,
    setup_requires=[
        "wheel==0.38.4",
        "Cython==0.29.32",
    ],
    install_requires=[
        "numpy==1.23.4",
        "cryptography==37.0.4",
        "aiohttp==3.8.3",
        "asyncpg==0.27.0",
        "uvloop==0.17.0",
        "asyncio==3.4.3",
        "cchardet==2.1.7",
        "pandas==1.5.3",
        "xlrd==2.0.1",
        "openpyxl==3.0.10",
        "lz4==4.0.2",
        "typing_extensions==4.4.0",
        "urllib3==1.26.14",
        "charset-normalizer>=2.0.7",
        "ciso8601==2.2.0",
        "iso8601==1.1.0",
        "pgpy==0.6.0",
        "python-magic==0.4.27",
        "dateparser==1.1.4",
        "python-datamodel>=0.1.14",
        "aiosqlite>=0.15.0",
    ],
    extras_require={
        "default": [
            "aioredis==2.0.1",
            "pylibmc==1.6.3",
            "aiomcache==0.8.0",
            "aiosqlite>=0.15.0",
            "cassandra-driver==3.25.0",
            "rethinkdb==2.4.9",
            "influxdb==5.3.1",
            "influxdb-client==1.35.0",
            "pymssql==2.2.7",
        ],
        "dataframe": [
            "dask==2023.1.1",
            "datatable==1.0.0",
            "polars==0.16.2",
            "pyarrow==11.0.0",
            "connectorx==0.3.1",
            "pyspark==3.3.1"
        ],
        "pyspark": [
            "pyspark==3.3.1"
        ],
        "sqlite": [
            "aiosqlite>=0.15.0",
        ],
        "memcache": [
            "pylibmc==1.6.3",
            "aiomcache==0.8.0",
        ],
        "redis": [
            "jsonpath-rw==1.4.0",
            "jsonpath-rw-ext==1.2.2",
            "redis==4.4.2",
            "aioredis==2.0.1",
            "hiredis==2.2.1",
            "objectpath==0.6.1",
        ],
        "rethinkdb": [
            "rethinkdb==2.4.9",
        ],
        "postgres": [
            "aiopg==1.4.0",
            "psycopg2-binary>=2.9.1",
        ],
        "postgresql": [
            "asyncpg==0.27.0",
        ],
        "mysql": [
            "asyncmy==0.2.5",
            "mysqlclient==2.1.1"
        ],
        "mariadb": [
            "aiomysql==0.1.1"
        ],
        "boto3": [
            "botocore==1.27.59",
            "boto3==1.24.59",
            "aiobotocore==2.4.2",
            "aioboto3==10.4.0"
        ],
        "cassandra": [
            "cassandra-driver==3.25.0",
        ],
        "influxdb": [
            "influxdb==5.3.1",
            "influxdb-client==1.35.0",
        ],
        "odbc": [
            "aioodbc==0.3.3",
            "pyodbc==4.0.35",
        ],
        "jdbc": [
            "JayDeBeApi==1.2.3"
        ],
        "oracle": [
            "oracledb==1.2.2"
        ],
        "sqlalchemy": [
            "sqlalchemy==1.4.46",
        ],
        "elasticsearch": [
            "elasticsearch[async]==8.6.1",
        ],
        "mongodb": [
            "pymongo==4.3.3",
            "motor==3.0.0",
        ],
        "msqlserver": [
            "pymssql==2.2.7",
        ],
        "couchbase": [
            "couchbase==4.0.5"
        ],
        "couchdb": [
            "aiocouch==2.2.2"
        ],
        "hazelcast": [
            "hazelcast-python-client==5.1"
        ],
        "all": [
            "dask==2023.1.1",
            "datatable==1.0.0",
            "polars==0.16.2",
            "pyarrow==11.0.0",
            "connectorx==0.3.1",
            "aiosqlite>=0.15.0",
            "pylibmc==1.6.3",
            "aiomcache==0.8.0",
            "jsonpath-rw==1.4.0",
            "jsonpath-rw-ext==1.2.2",
            "aioredis==2.0.1",
            "objectpath==0.6.1",
            "rethinkdb==2.4.9",
            "aiopg==1.4.0",
            "psycopg2-binary>=2.9.1",
            "cassandra-driver==3.25.0",
            "influxdb==5.3.1",
            "influxdb-client==1.35.0",
            "aioodbc==0.3.3",
            "JayDeBeApi==1.2.3",
            "pyodbc==4.0.35",
            "sqlalchemy==1.4.46",
            "elasticsearch[async]==8.6.1",
            "pymongo==4.3.3",
            "motor==3.1.1",
            "pymssql==2.2.7",
            "couchbase==4.0.5",
            "aiocouch==2.2.2",
            "asyncmy==0.2.5",
            "mysqlclient==2.1.1",
            "aiomysql==0.1.1",
            "pyspark==3.3.1",
            "oracledb==1.2.2",
            "hazelcast-python-client==5.1",
        ]
    },
    tests_require=[
        'pytest>=6.0.0',
        'pytest-asyncio==0.20.3',
        'pytest-xdist==3.1.0',
        'pytest-assume==2.4.2'
    ],
    test_suite='tests',
    ext_modules=cythonize(extensions),
    project_urls={  # Optional
        "Source": "https://github.com/phenobarbital/asyncdb",
        "Funding": "https://paypal.me/phenobarbital",
        "Say Thanks!": "https://saythanks.io/to/phenobarbital",
    },
)
