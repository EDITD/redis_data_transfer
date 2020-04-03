from contextlib import contextmanager
from datetime import datetime


class StatsTracker:
    def __init__(self, name, results_queue):
        self.name = name
        self.results = results_queue
        self.start_times = {}

    @contextmanager
    def track(self, reference):
        start_time = datetime.now()

        yield

        elapsed = datetime.now() - start_time
        self.results.put((self.name, reference, elapsed))

    def increment(self, reference):
        self.results.put((self.name, reference, 1))
