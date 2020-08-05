aw-core
=======

[![GitHub Actions badge](https://github.com/ActivityWatch/aw-core/workflows/Build/badge.svg)](https://github.com/ActivityWatch/aw-core/actions)
[![Code coverage](https://codecov.io/gh/ActivityWatch/aw-core/branch/master/graph/badge.svg)](https://codecov.io/gh/ActivityWatch/aw-core)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Typechecking: Mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


Core library for ActivityWatch.


## Modules

 - `aw_core`, contains basic datatypes and utilities, such as the `Event` class, helpers for configuration and logging, as well as schemas for buckets, events, and exports.
 - `aw_datastore`, contains the datastore classes used by aw-server-python.
 - `aw_transform`, all event-transforms used in queries.
 - `aw_query`, the query-language used by ActivityWatch.


## How to install

To install the latest git version directly from github without cloning, run
`pip install git+https://github.com/ActivityWatch/aw-core.git`

To install from a cloned version, cd into the directory and run
`poetry install` to install inside an virtualenv. If you want to install it
system-wide it can be installed with `pip install .`, but that has the issue
that it might not get the exact version of the dependencies due to not reading
the poetry.lock file.

