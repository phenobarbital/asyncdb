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
    python_requires=">=3.9.13",
    url="https://github.com/phenobarbital/asyncdb",
    description=__description__,
    keywords=[
        'asyncio',
        'asyncpg',
        'aioredis',
        'aiomcache',
        'cassandra',
        'scylladb'
    ],
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
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
        "Framework :: AsyncIO",
    ],
    author=__author__,
    author_email=__author_email__,
    packages=find_packages(exclude=["bin", "contrib", "docs", "tests", "examples", "libraries"]),
    package_data={"asyncdb": ["py.typed"]},
    license=__license__,
    setup_requires=[
        "setuptools==74.0.0",
        "Cython==3.0.11",
        "wheel==0.44.0"
    ],
    install_requires=[
        "cryptography==43.0.1",
        "aiohttp>=3.9.5",
        "asyncpg==0.29.0",
        "asyncio==3.4.3",
        "pandas==2.2.2",
        "xlrd==2.0.1",
        "openpyxl==3.1.2",
        "lz4==4.3.2",
        "charset-normalizer>=2.0.7",
        "iso8601==2.1.0",
        "pgpy==0.6.0",
        "python-magic==0.4.27",
        "dateparser==1.1.8",
        "python-datamodel>=0.7.0",
        "aiosqlite>=0.18.0",
        "looseversion==1.3.0",
        "aiofiles==24.1.0"
    ],
    extras_require={
        "uvloop": [
            "uvloop==0.20.0"
        ],
        "default": [
            "uvloop==0.20.0",
            "pylibmc==1.6.3",
            "aiomcache==0.8.2",
            "aiosqlite>=0.18.0",
            "cassandra-driver==3.29.1",
            "rethinkdb==2.4.10.post1",
            "influxdb==5.3.2",
            "influxdb-client[async]==1.45.0",
            "pymssql==2.3.1",
            "redis==5.0.8",
            "deltalake==0.19.2",
            "duckdb==1.1.0",
        ],
        "dataframe": [
            "dask==2024.8.2",
            'datatable==1.1.0',
            "python-datatable==1.1.3",
            "polars==1.7.0",
            "pyarrow==17.0.0",
            "connectorx==0.3.3",
            "pyspark==3.5.0",
            "deltalake==0.19.2",
            "duckdb==1.1.0",
        ],
        "pyspark": [
            "pyspark==3.5.0"
        ],
        "sqlite": [
            "aiosqlite>=0.18.0",
        ],
        "memcache": [
            "pylibmc==1.6.3",
            "aiomcache==0.8.2",
        ],
        "redis": [
            "jsonpath-rw==1.4.0",
            "jsonpath-rw-ext==1.2.2",
            "redis==5.0.8",
            "objectpath==0.6.1",
        ],
        "rethinkdb": [
            "rethinkdb==2.4.10.post1",
        ],
        "postgres": [
            "aiopg==1.4.0",
            "psycopg-binary>=3.1.8",
        ],
        "postgresql": [
            "asyncpg==0.29.0",
            "psycopg[binary,pool]>=3.1.8",
        ],
        "mysql": [
            "asyncmy==0.2.9",
            "mysqlclient==2.2.0"
        ],
        "mariadb": [
            "aiomysql==0.2.0"
        ],
        "boto3": [
            "botocore==1.31.64",
            "aiobotocore==2.7.0",
            "aioboto3==12.0.0"
        ],
        "bigquery": [
          "google-cloud-bigquery==3.25.0",
          "pandas-gbq==0.23.1",
          "google-cloud-storage==2.17.0"
        ],
        "cassandra": [
            "cassandra-driver==3.29.1",
        ],
        "influxdb": [
            "influxdb==5.3.2",
            "influxdb-client[async]==1.45.0",
        ],
        "odbc": [
            "aioodbc==0.5.0",
            "pyodbc==5.1.0",
        ],
        "jdbc": [
            "JayDeBeApi==1.2.3"
        ],
        "oracle": [
            "oracledb==2.2.1"
        ],
        "sqlalchemy": [
            "sqlalchemy[asyncio]==2.0.34",
        ],
        "elasticsearch": [
            "elasticsearch[async]==8.13.2",
        ],
        "mongodb": [
            "pymongo==4.6.1",
            "motor==3.4.0",
        ],
        "msqlserver": [
            "pymssql==2.3.1",
        ],
        "couchdb": [
            "aiocouch==3.0.0"
        ],
        "hazelcast": [
            "hazelcast-python-client==5.4.0"
        ],
        "scylla": [
            "scylla_driver==3.26.9",
            "cassandra-driver==3.29.1",
            "acsylla==0.2.0",
            "cqlsh==6.1.2"
        ],
        "clickhouse": [
            "clickhouse-driver==0.2.9",
            "clickhouse-cityhash==1.0.2.4",
            "aiochclient[httpx-speedups]==2.6.0",
            "clickhouse-connect==0.7.19"  # Support for SuperSet
        ],
        "redpanda": [
            "aiokafka==0.11.0"
        ],
        "all": [
            "dask==2024.8.2",
            "datatable==1.1.0",
            "python-datatable==1.1.3",
            "polars==1.7.0",
            "pyarrow==17.0.0",
            "connectorx==0.3.3",
            "aiosqlite>=0.18.0",
            "pylibmc==1.6.3",
            "aiomcache==0.8.2",
            "jsonpath-rw==1.4.0",
            "jsonpath-rw-ext==1.2.2",
            "redis==5.0.8",
            "objectpath==0.6.1",
            "rethinkdb==2.4.10.post1",
            "aiopg==1.4.0",
            "psycopg-binary>=3.1.8",
            "cassandra-driver==3.29.1",
            "scylla_driver==3.26.9",
            "acsylla==0.2.0",
            "cqlsh==6.1.2",
            "influxdb==5.3.2",
            "influxdb-client[async]==1.45.0",
            "aioodbc==0.5.0",
            "JayDeBeApi==1.2.3",
            "pyodbc==5.1.0",
            "sqlalchemy[asyncio]==2.0.34",
            "elasticsearch[async]==8.13.2",
            "pymongo==4.6.1",
            "motor==3.4.0",
            "pymssql==2.3.1",
            "aiocouch==3.0.0",
            "asyncmy==0.2.9",
            "mysqlclient==2.2.0",
            "aiomysql==0.2.0",
            "pyspark==3.5.0",
            "oracledb==2.2.1",
            "hazelcast-python-client==5.4.0",
            "deltalake==0.19.2",
            "duckdb==1.1.0",
            "botocore==1.31.64",
            "aiobotocore==2.7.0",
            "aioboto3==12.0.0",
            "google-cloud-bigquery==3.25.0",
            "google-cloud-storage==2.17.0",
            "pandas-gbq==0.23.1",
            "tqdm==4.66.4"
        ]
    },
    tests_require=[
        'pytest>=7.2.2',
        'pytest-asyncio==0.24.0',
        'pytest-xdist==3.6.1',
        'pytest-assume==2.4.3'
    ],
    test_suite='tests',
    ext_modules=cythonize(extensions),
    project_urls={  # Optional
        "Source": "https://github.com/phenobarbital/asyncdb",
        "Funding": "https://paypal.me/phenobarbital",
        "Say Thanks!": "https://saythanks.io/to/phenobarbital",
    },
)
