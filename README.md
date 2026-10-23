aw-core
=======

[![GitHub Actions badge](https://github.com/ActivityWatch/aw-core/workflows/Build/badge.svg)](https://github.com/ActivityWatch/aw-core/actions)
[![Code coverage](https://codecov.io/gh/ActivityWatch/aw-core/branch/master/graph/badge.svg)](https://codecov.io/gh/ActivityWatch/aw-core)
[![PyPI](https://img.shields.io/pypi/v/aw-core)](https://pypi.org/project/aw-core/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Typechecking: Mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


Core library for ActivityWatch.


## Modules

 - `aw_core`, contains basic datatypes and utilities, such as the `Event` class, helpers for configuration and logging, as well as schemas for buckets, events, and exports.
 - `aw_datastore`, contains the datastore classes used by aw-server-python.
 - `aw_transform`, all event-transforms used in queries.
 - `aw_query`, the query-language used by ActivityWatch.

## Device-scoped SQLite handles (draft)

The device-identity follow-up to the v2 schema adds
`Datastore.for_device(device_id)` and `SqliteStorage.for_device(device_id)`.
A scoped handle shares the SQLite connection and commit owner, but not another
device's Bucket-object cache. Bucket metadata and every event operation resolve
within that device. Unscoped ambiguous names raise `ValueError` before mutation;
`SqliteStorage.device_buckets()` returns lossless device-grouped metadata.

Handles retain SQLite's existing single-thread ownership. Unsupported Memory
and Peewee backends raise `NotImplementedError` for scoped routing rather than
falling back to global names. SQLite lookup supports exact-first bidirectional
hostname-metadata aliases, including complete underscore-bearing hostnames;
creation cannot split an existing alias or redirect a global alias to a peer.
Stored names are never rewritten. Unknown hostnames do not create aliases.
This is storage groundwork, not Python HTTP parity: server routes, UUID
normalization, query/heartbeat caches and grouped HTTP transfer remain follow-up
integration work. Metadata-repair/alias interactions still need the shared
acceptance matrix. It does not enable watcher or
V2 rollout. Existing names and the underlying schema are unchanged by this slice.

## Logging

Run python with `LOG_LEVEL=debug` to use change the log level across all AW components

## How to install

To install the latest git version directly from github without cloning, run
`pip install git+https://github.com/ActivityWatch/aw-core.git`

To install from a cloned version, cd into the directory and run
`poetry install` to install inside an virtualenv. If you want to install it
system-wide it can be installed with `pip install .`, but that has the issue
that it might not get the exact version of the dependencies due to not reading
the poetry.lock file.
