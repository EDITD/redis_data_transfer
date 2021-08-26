from collections import defaultdict
from datetime import datetime, timedelta
from multiprocessing import Event
import os
import queue

from redis_data_transfer.processing import BaseProcess


class Display(BaseProcess):
    def __init__(self, name, tracker_queue, log_queue, refresh_interval):
        super(Display, self).__init__(name, tracker_queue, log_queue)
        self.events_queue = tracker_queue
        self.interval = refresh_interval
        self._stop = Event()
        self.state = defaultdict(dict)

    def execute(self):
        while not (self._stop.is_set() and self.events_queue.empty()):
            self._render_result()
            self._process_events()
        self._render_result()

    def stop(self, join=True):
        self._stop.set()
        if join:
            self.join()

    def _process_events(self):
        deadline = datetime.now() + timedelta(seconds=self.interval)
        while True:
            try:
                timeout = deadline - datetime.now()
                timeout_seconds = timeout.total_seconds()
                if timeout_seconds < 0:
                    return
                process, reference, value = self.events_queue.get(True, timeout_seconds)
            except queue.Empty:
                return

            previous_value = self.state[process].get(reference)
            if previous_value is None:
                previous_value = value.__class__(0)

            self.state[process][reference] = previous_value + value

    def _render_result(self):
        os.system('clear')
        sorted_keys = sorted(
            self.state.keys(),
            key=lambda name: (
                {'c': 0, 's': -1, 'r': 1, 'w': 2, 'g': 3}[name[0]],
                int(name.rsplit('_', maxsplit=1)[-1]),
            ),
        )
        for name in sorted_keys:
            raw_results = self.state[name].copy()
            batches = raw_results.get('batches')

            if batches:
                averages = {}
                for key, value in raw_results.items():
                    if key != 'batches':
                        averages[f'{key}_avg'] = value / batches
                raw_results.update(averages)

            res = {}
            for key, value in raw_results.items():
                if isinstance(value, timedelta):
                    minutes = value.seconds // 60
                    seconds = value.seconds - 60 * minutes
                    res[key] = f'{minutes:>3}:{seconds:02}.{value.microseconds:<06}'
                elif isinstance(value, int):
                    res[key] = f'{value:>6}'
                elif isinstance(value, float):
                    res[key] = f'{value:>9.1f}'
                else:
                    res[key] = f'{value}'

            self.info(
                '%s %s',
                f'{name:<10}',
                ''.join(f'{key} = {res[key]}  ' for key in sorted(res.keys())),
            )
