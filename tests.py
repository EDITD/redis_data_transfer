import contextlib
import functools
import os
import queue

import docker
import redis

from redis_data_transfer import move_data


REDIS_DOCKER_IMAGE = "redis:5-alpine"


@contextlib.contextmanager
def redis_server(container_name):
    full_container_name = "redis_data_transfer_test_redis_{}_{}".format(container_name, os.getpid())
    client = docker.from_env()

    exposed_port = "6379/tcp"
    container = None
    try:
        container = client.containers.run(
            REDIS_DOCKER_IMAGE,
            remove=True,
            detach=True,
            ports={exposed_port: None},
            name=full_container_name,
        )
        # The docker-py API is not great to get the host port
        # See https://github.com/docker/docker-py/issues/1451
        host_ports = client.api.inspect_container(container.id)["NetworkSettings"]["Ports"]
        yield int(host_ports[exposed_port][0]['HostPort'])
    finally:
        if container is not None:
            container.remove(force=True)


def add_redis_server(server_name):
    def decorator(test_func):
        @functools.wraps(test_func)
        def wrapper(*args, **kwargs):
            with redis_server(server_name) as redis_server_ports:
                kwargs[f'{server_name}_port'] = redis_server_ports
                return test_func(*args, **kwargs)
        return wrapper
    return decorator


@add_redis_server("source")
@add_redis_server("destination")
def test_copy_simple(source_port, destination_port):
    _check_move_data(
        source_port, destination_port,
        count=None, batch_size=1000,
        num_readers=1, num_writers=1,
        sample_size=1000,
    )


@add_redis_server("source")
@add_redis_server("destination")
def test_copy_with_count(source_port, destination_port):
    _check_move_data(
        source_port, destination_port,
        count=100, batch_size=10000,
        num_readers=1, num_writers=1,
        sample_size=1000,
    )


@add_redis_server("source")
@add_redis_server("destination")
def test_copy_multiple_readers_and_writers(source_port, destination_port):
    _check_move_data(
        source_port, destination_port,
        count=None, batch_size=10000,
        num_readers=3, num_writers=3,
        sample_size=1000,
    )


@add_redis_server("source")
@add_redis_server("destination")
def test_copy_many_batches(source_port, destination_port):
    _check_move_data(
        source_port, destination_port,
        count=None, batch_size=100,
        num_readers=1, num_writers=1,
        sample_size=10000,
    )


def _check_move_data(
        source_port, destination_port,
        count, batch_size,
        num_readers, num_writers,
        sample_size,
):
    source = redis.Redis(host="127.0.0.1", port=source_port)
    destination = redis.Redis(host="127.0.0.1", port=destination_port)

    assert 0 == source.dbsize()
    assert 0 == destination.dbsize()

    num_inserted = _insert_fake_data(source, sample_size)

    assert num_inserted == source.dbsize()
    assert 0 == destination.dbsize()

    dummy_log_queue = queue.Queue()
    move_data(
        source=f'127.0.0.1:{source_port}',
        destination=f'127.0.0.1:{destination_port}',
        count=count,
        batch_size=batch_size,
        num_readers=num_readers,
        num_writers=num_writers,
        log_queue=dummy_log_queue,
        track_items=False,
        refresh_interval=1.0,
    )

    assert num_inserted == source.dbsize()

    if count is None:
        expected_transfered = num_inserted
    else:
        expected_transfered = min(count, num_inserted)

    assert expected_transfered == destination.dbsize()


def _insert_fake_data(client, sample_size):
    pipe = client.pipeline()
    keys_created = 0

    for key, value in [(f"key_{i}", f"value_{i}") for i in range(sample_size)]:
        pipe.set(key, value)
        keys_created += 1

    for key, value in [(f"key_{i}", f"value_{i}") for i in range(sample_size)]:
        pipe.hset("test_hash", key, value)
    keys_created += 1

    for key, value in [(f"key_{i}", f"value_{i}") for i in range(sample_size)]:
        pipe.sadd("test_set", key, value)
    keys_created += 1

    pipe.execute()
    return keys_created
