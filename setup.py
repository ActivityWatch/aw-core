#!/usr/bin/env python

import os
import re
from typing import Any

from setuptools import setup


base_dir = os.path.dirname(__file__)

about = {}  # type: Any
with open(os.path.join(base_dir, "aw_core", "__about__.py")) as f:
    exec(f.read(), about)

with open(os.path.join(base_dir, "README.md")) as f:
    long_description = f.read()

if False:
    with open(os.path.join(base_dir, "CHANGELOG.rst")) as f:
        # Remove :issue:`ddd` tags that breaks the description rendering
        changelog = re.sub(
            r":issue:`(\d+)`",
            r"`#\1 <https://github.com/pypa/pipfile/issues/\1>`__",
            f.read(),
        )
        long_description = "\n".join([long_description, changelog])


setup(name              =about["__title__"],
      version           =about["__version__"],
      description       =about["__summary__"],
      long_description  =long_description,
      author            =about["__author__"],
      author_email      =about["__email__"],
      url               =about["__uri__"],
      packages=set(["aw_core", "aw_datastore"]),
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
