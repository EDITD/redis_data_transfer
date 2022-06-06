from logging.handlers import QueueListener
from multiprocessing import Queue
import argparse
import logging

from redis_data_transfer.display import Display
from redis_data_transfer.processing import Drain, Processor, Source, TombStone
from redis_data_transfer.redis_client import _redis_client
from redis_data_transfer.state import StatsTracker


def main():
    parser = argparse.ArgumentParser("Move data from redis(-cluster) to redis(-cluster)")

    parser.add_argument('source', help="Source server as hostname[:port][#database]")
    parser.add_argument('destination', help="Destination server as  as hostname[:port][#database]")
    parser.add_argument('--count', help="Number of key/values to copy", default=None, type=int)
    parser.add_argument('--batch', help="Number of key/values per batch", default=10000, type=int)
    parser.add_argument('--checkers', help='Number of checker processes', default=0, type=int)
    parser.add_argument('--readers', help='Number of reader processes', default=1, type=int)
    parser.add_argument('--writers', help='Number of writer processes', default=1, type=int)
    parser.add_argument('--track-items', help='Track each item processed',
                        dest='track_items', action='store_true')
    parser.add_argument('--no-track-items',
                        dest='track_items', action='store_false')
    parser.set_defaults(track_items=False)
    parser.add_argument('--refresh-interval', help='Status refresh interval in seconds',
                        default=1.0, type=float)
    args = parser.parse_args()

    log_queue = _configure_logging()

    move_data(
        args.source, args.destination,
        args.count, args.batch,
        args.checkers, args.readers, args.writers,
        log_queue,
        args.track_items,
        args.refresh_interval,
    )


def _configure_logging():
    logging.basicConfig(level=logging.DEBUG)
    log_queue = Queue()
    log_listener = QueueListener(log_queue)
    log_listener.start()
    return log_queue


def move_data(
        source, destination,
        count, batch_size,
        num_checkers, num_readers, num_writers,
        log_queue,
        track_items, refresh_interval,
):
    read_queue = Queue(maxsize=num_readers * 4)
    write_queue = Queue(maxsize=num_writers * 4)
    tracker_queue = Queue()

    if num_checkers:
        check_queue = Queue(maxsize=num_checkers * 4)

        checkers = [
            RedisChecker(f'checker_{i}', destination, check_queue, read_queue, tracker_queue, log_queue, track_items)
            for i in range(num_checkers)
        ]
        for checker in checkers:
            checker.start()

        scanner_destination = check_queue
    else:
        scanner_destination = read_queue

    scanner = RedisScanner(source, count, batch_size, scanner_destination, tracker_queue, log_queue, track_items)
    scanner.start()

    readers = [
        RedisReader(f'reader_{i}', source, read_queue, write_queue, tracker_queue, log_queue, track_items)
        for i in range(num_readers)
    ]
    for reader in readers:
        reader.start()

    writers = [
        RedisInserter(f'writer_{i}', destination, log_queue, write_queue, tracker_queue, track_items)
        for i in range(num_writers)
    ]
    for writer in writers:
        writer.start()

    displayer = Display('display', tracker_queue, log_queue, refresh_interval)
    displayer.start()

    tracker = StatsTracker('global_0', tracker_queue)

    with tracker.track('process'):
        scanner.join()

        if num_checkers:
            for _ in range(num_checkers):
                check_queue.put(TombStone())

            for checker in checkers:
                checker.join()

        for _ in range(num_readers):
            read_queue.put(TombStone())

        for reader in readers:
            reader.join()

        for _ in range(num_writers):
            write_queue.put(TombStone())

        for writer in writers:
            writer.join()

    displayer.stop()


class RedisScanner(Source):
    def __init__(self, source, count, batch_size, read_queue, results, log_queue, track_items):
        super(RedisScanner, self).__init__(
            'scanner_0', results, log_queue, read_queue, count, batch_size, track_items,
        )
        redis = _redis_client(source, self.logger)
        self.scan_iter = redis.scan_iter(count=batch_size)

    def produce_item(self):
        try:
            return next(self.scan_iter)
        except StopIteration:
            return None


class RedisChecker(Processor):
    def __init__(self, name, target_host, check_queue, read_queue, results, log_queue, track_items):
        super(RedisChecker, self).__init__(
            name, results, log_queue, check_queue, read_queue, track_items,
        )
        redis = _redis_client(target_host, self.logger)
        self.pipe = redis.pipeline()

    def process_item(self, item):
        self.pipe.exists(item)
        return True

    def finalise_batch(self, batch):
        results = self.pipe.execute()
        return [key for key, exists in zip(batch, results) if not exists]


class RedisReader(Processor):
    def __init__(self, name, source, read_queue, write_queue, results, log_queue, track_items):
        super(RedisReader, self).__init__(
            name, results, log_queue, read_queue, write_queue, track_items,
        )
        redis = _redis_client(source, self.logger)
        self.pipe = redis.pipeline()

    def process_item(self, item):
        self.pipe.dump(item)
        return True

    def finalise_batch(self, batch):
        values = self.pipe.execute()
        return list(zip(batch, values))


class RedisInserter(Drain):
    def __init__(self, name, target_host, log_queue, input_queue, results, track_items):
        super(RedisInserter, self).__init__(name, results, log_queue, input_queue, track_items)

        redis = _redis_client(target_host, self.logger)
        self.pipe = redis.pipeline()

    def process_item(self, item):
        key, value = item
        if value is not None:
            self.pipe.restore(key, 0, value, replace=False)
            return True

    def finalise_batch(self, _batch):
        self.pipe.execute()
