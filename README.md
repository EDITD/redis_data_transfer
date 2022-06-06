[![Build Status](https://travis-ci.org/EDITD/redis_data_transfer.svg?branch=master)](https://travis-ci.org/EDITD/redis_data_transfer)
[![Pypi Version](https://img.shields.io/pypi/v/redis_data_transfer.svg)](https://pypi.org/project/redis_data_transfer/)
[![Python Versions](https://img.shields.io/pypi/pyversions/redis_data_transfer.svg)](https://pypi.org/project/redis_data_transfer/)

# Redis data transfer tool

An easy-to-use tool to transfer data between redis servers or clusters.

### Installation
```pip install redis-data-transfer```

### Usage

The command line structure is quite simple:
```redis-data-transfer [options] your.source.server your.destination.server```

For details about the options available:
```redis-data-transfer --help```

### Concepts

The implementation is made around a pipeline system with queues and subprocesses. The user can
control the number of parallel subprocesses for each step of the pipeline:
* A single scanner reads all keys from the source.
* Checkers look in the destination and filter out any key that already exists. They can be disabled
  if desired.
* Readers fetch the content of each key from the source.
* Writers store the content for each key in the destination.


## Development

The code is [hosted on github](https://github.com/EDITD/redis_data_transfer).
The repository uses [poetry](https://python-poetry.org/) for packaging.
The project uses [tox](https://tox.wiki/en/latest/) for testing.
