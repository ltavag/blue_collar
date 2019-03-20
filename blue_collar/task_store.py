from multiprocessing import RLock
from collections import defaultdict
import lmdb
from .util import json_serial
import json
import dateutil.parser
import datetime

STATS_KEY = 'global_stats'
STATS_LOCK = RLock()


class TaskStore:
    """A small wrapper class for LMDB acesss.

    LMDB is smart enough to block when multiple
    processes require write access, so this class
    can be instantiated as many times as needed.
    """
    def __init__(self, path='stuff.lmdb'):
        self.env = lmdb.open(path)
        self.stats = defaultdict(dict)

    def _put(self, key, value):
        with self.env.begin(write=True) as txn:
            txn.put(key.encode('utf-8'),
                    json.dumps(value,
                               sort_keys=True,
                               default=json_serial).encode('utf-8'))

    def _get(self, key):
        with self.env.begin(write=False) as txn:
            return txn.get(key.encode('utf-8'))

    def put(self, key, val):
        with STATS_LOCK:
            current_stats = self._get(STATS_KEY)
            if current_stats:
                current_stats = json.loads(current_stats)
            else:
                current_stats = {}

            """Check to see the command exists in the stats dict."""
            if val['command'] not in current_stats:
                current_stats[val['command']] = {}

            if 'status' not in current_stats[val['command']]:
                current_stats[val['command']]['status'] = {}

            if val['status'] not in current_stats[val['command']]['status']:
                current_stats[val['command']]['status'][val['status']] = []

            current_stats[val['command']]['status'][val['status']].append(key)

            """
            If the task already existed in the stats dict,
            make sure to update the previous status counter.
            """
            t = self._get(key)
            if t:
                t = json.loads(t)
                current_stats[t['command']]['status'][t['status']].remove(key)

                """
                If a task is finished, ensure that we
                increment the total count for time taken
                to complete tasks.
                """
                if val['status'] == 'finished':
                    start = dateutil.parser.parse(t['start'])
                    time_taken = (val['finish'] - start).seconds
                    if 'time_taken' not in current_stats[t['command']]:
                        current_stats[t['command']]['time_taken'] = 0

                    current_stats[t['command']]['time_taken'] += time_taken

                """
                If a task is being de-queued, increment the 
                total count for time in queue, so we can keep
                track of the average.
                """
                if t['status'] == 'queued':
                    start = dateutil.parser.parse(t['start'])
                    time_in_queue = (datetime.datetime.now() - start).seconds

                    if 'time_in_queue' not in current_stats[t['command']]:
                        current_stats[t['command']]['time_in_queue'] = 0

                    current_stats[t['command']]['time_in_queue'] += time_in_queue

            self._put(STATS_KEY, current_stats)

        self._put(key, val)

    def get(self, key):
        return self._get(key)

    def get_stats(self):
        return self._get(STATS_KEY)
