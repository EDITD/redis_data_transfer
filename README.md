# Redis data transfer tool

An easy-to-use tool to transfer data between redis servers or clusters.

### Installation
```pip install redis-data-transfer```

### Usage

The command line structure is quite simple:
```redis-data-transfer [options] your.source.server your.destination.server```

For details about the options available:
```redis-data-transfer --help```


## Development

The code is [hosted on github](https://github.com/EDITD/redis_data_transfer)
The repository uses [poetry](https://python-poetry.org/) for packaging.

### Docker image
The docker image allows to run the tool easily from hosts where running a docker container is easier
than installing a python tool.

To build the image:
```
docker build -t redis-data-transfer:vX.Y.Z .
```

To run the tool:
```
docker run --rm -it redis-data-transfer:vX.Y.Z [options] your.source.server your.destination.server
```