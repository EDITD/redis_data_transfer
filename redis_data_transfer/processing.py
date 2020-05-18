import logging
from logging.handlers import QueueHandler
from multiprocessing import Process
from queue import Empty

import setproctitle

from redis_data_transfer.state import StatsTracker


class TombStone:
    pass


class QueueLoggingMixin:
    def __init__(self, log_queue):
        log_handler = QueueHandler(log_queue)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(log_handler)

    def debug(self, *args, **kwargs):
        return self.logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        return self.logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        return self.logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        return self.logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        return self.logger.exception(*args, **kwargs)


class BaseProcess(Process, QueueLoggingMixin):
    def __init__(self, name, tracker_queue, log_queue):
        super(BaseProcess, self).__init__(name=name)
        QueueLoggingMixin.__init__(self, log_queue)
        self.tracker = StatsTracker(name, tracker_queue)

    def run(self):
        setproctitle.setproctitle(self.name)
        self.execute()

    def execute(self):
        raise NotImplementedError


class Source(BaseProcess):
    def __init__(
            self, name, tracker_queue, log_queue, target_queue, count, batch_size, track_items=True,
    ):
        super(Source, self).__init__(name, tracker_queue, log_queue)
        self.output = target_queue
        self.batch_size = batch_size
        self.count = count
        self.track_items = track_items

    def execute(self):
        while True:
            with self.tracker.track('process'):
                batch = self.produce_batch()
            if batch is None:
                break
            self.emit_batch(batch)

    def produce_batch(self):
        batch = []
        while len(batch) < self.batch_size:
            item = self._produce_item()
            if item is None:
                break
            if self.track_items:
                self.tracker.increment('items')
            batch.append(item)

        if len(batch) > 0:
            return batch

    def _produce_item(self):
        if self.count is not None and self.count <= 0:
            return None

        item = self.produce_item()

        if self.count is not None:
            self.count -= 1

        return item

    def produce_item(self):
        raise NotImplementedError

    def emit_batch(self, batch):
        with self.tracker.track('wait'):
            self.output.put(batch)
        self.tracker.increment('batches')


class Drain(BaseProcess):
    def __init__(self, name, tracker_queue, log_queue, input_queue, track_items=True):
        super(Drain, self).__init__(name, tracker_queue, log_queue)
        self.input = input_queue
        self.track_items = track_items

    def execute(self):
        while True:
            try:
                with self.tracker.track('wait'):
                    batch = self.input.get(True, 1.0)
            except Empty:
                continue
            else:
                if isinstance(batch, TombStone):
                    break

            with self.tracker.track('process'):
                self.process_batch(batch)

    def process_batch(self, batch) -> None:
        for item in batch:
            if self.process_item(item) and self.track_items:
                self.tracker.increment('items')
        results = self.finalise_batch(batch)
        self.process_results(results)
        self.tracker.increment('batches')

    def finalise_batch(self, batch):
        pass

    def process_item(self, item) -> bool:
        raise NotImplementedError

    def process_results(self, results):
        pass


class Processor(Drain):
    def __init__(self, name, tracker_queue, log_queue, input_queue, output_queue, track_items=True):
        super(Processor, self).__init__(name, tracker_queue, log_queue, input_queue, track_items)
        self.output = output_queue

    def process_results(self, results):
        with self.tracker.track('wait'):
            self.output.put(results)

    def process_item(self, item) -> bool:
        raise NotImplementedError
