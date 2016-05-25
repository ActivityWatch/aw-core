#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='aw-core',
      version='0.1',
      description='Core library for ActivityWatch',
      author='Erik Bjäreholt',
      author_email='erik@bjareho.lt',
      url='https://github.com/ActivityWatch/aw-core',
      namespace_packages=['aw'],
      packages=['aw.core'],
      install_requires=['jsonschema', 'strict-rfc3339']
     )
