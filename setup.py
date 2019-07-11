#!/usr/bin/env python

import os
import re
from typing import Any

from setuptools import setup


base_dir = os.path.dirname(__file__)

about = {}  # type: Any
with open(os.path.join(base_dir, "aw_core", "__about__.py"), "r", encoding="utf-8") as f:
    exec(f.read(), about)

with open(os.path.join(base_dir, "README.md"), "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    install_requires = list(filter(lambda l: not l.startswith("-i"), f.readlines()))

setup(name=about["__title__"],
      version=about["__version__"],
      description=about["__summary__"],
      long_description=long_description,
      author=about["__author__"],
      author_email=about["__email__"],
      url=about["__uri__"],
      packages=set(["aw_core", "aw_transform", "aw_query", "aw_datastore", "aw_datastore.storages"]),
      package_data={"aw_core": ['schemas/*.json']},
      install_requires=install_requires,
      classifiers=[
          'Programming Language :: Python :: 3']
      )
