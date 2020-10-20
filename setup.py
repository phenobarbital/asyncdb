#!/usr/bin/env python
"""AsyncDB
    Asynchronous library for data source connections, used by Navigator.
See:
https://bitbucket.org/mobileinsight1/asyncdb/src/master/
"""

from setuptools import find_packages, setup

setup(
    name="asyncdb",
    version="1.0.0",
    url="https://bitbucket.org/mobileinsight1/asyncdb/",
    description="Asyncio Datasource library",
    long_description="Asynchronous library for data source connections, used by Navigator",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Programming Language :: Python :: 3.7",
    ],
    author="Jesus Lara",
    author_email="jlara@trocglobal.com",
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=["numpy >= 1.11.1", "asyncio==3.4.3", "python-dateutil==2.8.1"],
    project_urls={  # Optional
        "Source": "https://bitbucket.org/mobileinsight1/asyncdb/",
        "Funding": "https://paypal.me/phenobarbital",
        "Say Thanks!": "https://saythanks.io/to/phenobarbital",
    },
)
