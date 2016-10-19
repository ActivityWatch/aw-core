#!/usr/bin/env python

from setuptools import setup

setup(name='aw-core',
      version='0.2',
      description='Core library for ActivityWatch',
      author='Erik Bjäreholt',
      author_email='erik@bjareho.lt',
      url='https://github.com/ActivityWatch/aw-core',
      packages=['aw_core', 'aw_datastore'],
      install_requires=[
          'jsonschema',
          'peewee>=2.8.5',
          'strict-rfc3339',
          'appdirs==1.4.0',
          'nose-parameterized',
          'iso8601',
          'tinydb>=3.2.1',
          'tinydb-serialization>=1.0.3',
          'python-json-logger>=0.1.5',
          'TakeTheTime>=0.2.0',
      ])
