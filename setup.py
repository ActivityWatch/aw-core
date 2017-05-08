#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='aw-core',
      version='0.3',
      description='Core library for ActivityWatch',
      author='Erik Bjäreholt, Johan Bjäreholt',
      author_email='erik@bjareho.lt, johan@bjareho.lt',
      url='https://github.com/ActivityWatch/aw-core',
      packages=set(["aw_core", "aw_database"]),
      install_requires=[
          'jsonschema',
          'peewee>=2.8.5',
          'strict-rfc3339',
          'appdirs>=1.4.0',
          'iso8601',
          'tinydb>=3.2.1',
          'tinydb-serialization>=1.0.3',
          'python-json-logger>=0.1.5',
          'takethetime>=0.3.0',
      ],
      classifiers=[
          'Programming Language :: Python :: 3'
      ])
