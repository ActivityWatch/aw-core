aw-core
=======

[![Build Status Travis](https://travis-ci.org/ActivityWatch/aw-core.svg?branch=master)](https://travis-ci.org/ActivityWatch/aw-core)
[![Build Status Appveyor](https://ci.appveyor.com/api/projects/status/h5cvxoghh1wr4ycr/branch/master?svg=true)](https://ci.appveyor.com/project/ErikBjare/aw-core/branch/master)
[![codecov](https://codecov.io/gh/ActivityWatch/aw-core/branch/master/graph/badge.svg)](https://codecov.io/gh/ActivityWatch/aw-core)


Core library for ActivityWatch.


## Contents

 - Models
 - Schemas
 - Filtering algorithms for sensitive data


## How to install

To install the latest git version directly from github without cloning, run
`pip install git+https://github.com/ActivityWatch/aw-core.git`

To install from a cloned version, cd into the directory and run
`poetry install` to install inside an virtualenv. If you want to install it
system-wide it can be installed with `pip install .`, but that has the issue
that it might not get the exact version of the dependencies due to not reading
the poetry.lock file.

