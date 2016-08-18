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
          'strict-rfc3339',
          'appdirs==1.4.0',
          'nose-parameterized',
          'iso8601',
          'pytz'
      ])
