#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='actwa-core',
      version='0.1',
      description='Core library for ActivityWatch',
      author='Erik Bjäreholt',
      author_email='erik@bjareho.lt',
      url='https://github.com/ActivityWatch/actwa-core',
      packages=['actwa.core'],
      namespace_packages=['actwa']
     )
