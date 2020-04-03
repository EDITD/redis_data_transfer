from rediscluster import RedisCluster
from rediscluster.exceptions import RedisClusterException, ResponseError
from redis import ConnectionError, Redis


CONNECT_TIMEOUT_SEC = 10


def _redis_client(host, logger):
    startup_node = _split_host(host)
    logger.debug("Host name split: %s", startup_node)

    try:
        client = RedisCluster(
            startup_nodes=[startup_node],
            socket_connect_timeout=CONNECT_TIMEOUT_SEC,
        )
        client.cluster('info')
        logger.info("Connected to cluster %s", host)
        return client
    except (RedisClusterException, ResponseError):
        pass

    try:
        client = Redis(
            **startup_node,
            socket_connect_timeout=CONNECT_TIMEOUT_SEC,
        )
        client.info()
        logger.info("Connected to host %s", host)
        return client
    except ConnectionError:
        pass

    logger.error("Could not connect to cluster/host", host)
    return None


def _split_host(host):
    if '#' in host:
        hostname_port, database = host.split('#', maxsplit=1)
    else:
        hostname_port = host
        database = "0"

    if ':' in hostname_port:
        hostname, port = hostname_port.split(':', maxsplit=1)
    else:
        hostname = hostname_port
        port = "6379"

    return {'host': hostname, 'port': port, 'db': int(database)}
