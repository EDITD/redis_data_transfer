import contextlib
import os
import queue
import unittest

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
        client.close()


class TestRedisDataTransfer(unittest.TestCase):
    def test_copy_simple(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            _check_move_data(
                source_port, destination_port,
                count=None, batch_size=1000,
                num_checkers=1, num_readers=1, num_writers=1,
                sample_size=1000,
            )

    def test_copy_with_count(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            _check_move_data(
                source_port, destination_port,
                count=100, batch_size=10000,
                num_checkers=1, num_readers=1, num_writers=1,
                sample_size=1000,
            )

    def test_copy_multiple_readers_and_writers(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            _check_move_data(
                source_port, destination_port,
                count=None, batch_size=10000,
                num_checkers=3, num_readers=3, num_writers=3,
                sample_size=1000,
            )

    def test_copy_no_checker(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            _check_move_data(
                source_port, destination_port,
                count=None, batch_size=10000,
                num_checkers=0, num_readers=1, num_writers=1,
                sample_size=1000,
            )

    def test_copy_many_batches(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            _check_move_data(
                source_port, destination_port,
                count=None, batch_size=100,
                num_checkers=1, num_readers=1, num_writers=1,
                sample_size=10000,
            )

    def test_copy_with_checker_and_preexisting_data(self):
        with redis_server("source") as source_port, redis_server("destination") as destination_port:
            batch_size = 100
            sample_size_partial = 100
            sample_size_total = 1000

            dummy_log_queue = queue.Queue()

            source = redis.Redis(host="127.0.0.1", port=source_port)
            destination = redis.Redis(host="127.0.0.1", port=destination_port)

            assert 0 == source.dbsize()
            assert 0 == destination.dbsize()

            num_inserted_source = _insert_fake_data(source, sample_size_total)

            assert num_inserted_source == source.dbsize()
            assert 0 == destination.dbsize()

            move_data(
                source=f'127.0.0.1:{source_port}',
                destination=f'127.0.0.1:{destination_port}',
                count=sample_size_partial,
                batch_size=batch_size,
                num_checkers=0,
                num_readers=1,
                num_writers=1,
                log_queue=dummy_log_queue,
                track_items=False,
                refresh_interval=1.0,
            )

            assert num_inserted_source == source.dbsize()
            assert sample_size_partial == destination.dbsize()

            move_data(
                source=f'127.0.0.1:{source_port}',
                destination=f'127.0.0.1:{destination_port}',
                count=None,
                batch_size=batch_size,
                num_checkers=1,
                num_readers=1,
                num_writers=1,
                log_queue=dummy_log_queue,
                track_items=False,
                refresh_interval=1.0,
            )

            assert num_inserted_source == source.dbsize()
            assert num_inserted_source == destination.dbsize()


def _check_move_data(
        source_port, destination_port,
        count, batch_size,
        num_checkers, num_readers, num_writers,
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
        num_checkers=num_checkers,
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
