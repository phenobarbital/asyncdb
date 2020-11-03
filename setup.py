#!/usr/bin/env python
"""AsyncDB.

    Asynchronous library for data source connections, used by Navigator.
See:
https://github.com/phenobarbital/asyncdb
"""

from setuptools import find_packages, setup

setup(
    name="asyncdb",
    version=open("VERSION").read().strip(),
    python_requires=">=3.7.0",
    url="https://github.com/phenobarbital/asyncdb",
    description="Asyncio Datasource library",
    long_description="Asynchronous library for data source connections, \
    used by Navigator",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Programming Language :: Python :: 3.7",
    ],
    author="Jesus Lara",
    author_email="jlara@trocglobal.com",
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=[
        "numpy >= 1.11.1",
        "wheel==0.35.1",
        "asyncio==3.4.3",
        "PyDrive==1.3.1",
        "uvloop==0.14.0",
        "objectpath==0.6.1",
        "MarkupSafe==1.1.1",
        "jinja2==2.11.2",
        "jsonpath-rw==1.4.0",
        "jsonpath-rw-ext==1.2.2",
        "dateparser==0.7.6",
        "iso8601==0.1.13",
        "aiopg==1.0.0",
        "tqdm==4.50.2",
        "tabulate==0.8.7",
        "python-magic==0.4.18",
        "pgpy==0.5.3",
        "botocore==1.18.18",
        "boto3==1.15.18",
        "xlrd==1.2.0",
        "bs4==0.0.1",
        "pylibmc==1.6.1",
        "redis==3.5.3",
        "aioredis==1.3.1",
        "asyncpg==0.21.0",
        "rethinkdb==2.4.7",
        "openpyxl==3.0.5",
        "lxml==4.6.0",
        "isodate==0.6.0",
        "dask==2.30.0",
        "pandas==1.1.3",
        "hiredis==1.1.0",
        "aiomcache==0.6.0",
        "aiosqlite==0.15.0",
        "sqlalchemy==1.3.20",
        "sqlalchemy-aio==0.16.0",
        "redis==3.5.3",
        "pylibmc==1.6.1",
        "attrs==20.2.0",
        "aioodbc==0.3.3",
        "pyodbc==4.0.30",
        "python-rapidjson==0.9.3",
        "rapidjson==1.0.0"

    ],
    extra_requires=[
        'cryptography>=3.2'
    ],
    project_urls={  # Optional
        "Source": "https://github.com/phenobarbital/asyncdb",
        "Funding": "https://paypal.me/phenobarbital",
        "Say Thanks!": "https://saythanks.io/to/phenobarbital",
    },
)
