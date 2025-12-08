#!/usr/bin/env python
import os
from setuptools import setup, Command, find_packages

setup(
    name='tap-mubi',
	version="0.1.0",
	description="Singer tap for extracting data from Mubi's Top 1000 List",
	author="David Witkowski",
	classifiers=["Programming Language :: Python :: 3 :: Only"],
	py_modules=["tap_mubi"],
	install_requires=[
        'backoff==1.8.0',
        'certifi==2024.7.4',
        'charset-normalizer==3.0.1',
        'ciso8601==2.3.0',
        'idna==3.7',
        'jsonschema==2.6.0',
        'python-dateutil==2.8.2',
        'pytz==2022.7.1',
        'requests==2.32.4',
        'simplejson==3.11.1',
        'singer-python==5.13.0',
        'six==1.16.0',
        'urllib3==2.6.0'
	],
    packages=["tap_mubi"],
    package_dir={"tap_mubi": "tap_mubi"},
    package_data={'tap_mubi': ['schemas/*.json']},
	entry_points={
        'console_scripts': ['tap-mubi=tap_mubi:main']
        }
)
