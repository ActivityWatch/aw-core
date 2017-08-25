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

setup(name=about["__title__"],
      version=about["__version__"],
      description=about["__summary__"],
      long_description=long_description,
      author=about["__author__"],
      author_email=about["__email__"],
      url=about["__uri__"],
      packages=set(["aw_core", "aw_datastore", "aw_datastore.storages"]),
      package_data={"aw_core": ['schemas/*.json']},
      classifiers=[
          'Programming Language :: Python :: 3']
      )
