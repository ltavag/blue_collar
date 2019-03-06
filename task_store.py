import lmdb
from collections import defaultdict
from util import json_serial
import json

class TaskStore:
    """A small wrapper class for LMDB acesss.

    LMDB is smart enough to block when multiple
    processes require write access, so this class
    can be instantiated as many times as needed.
    """
    def __init__(self, path='stuff.lmdb'):
        self.env = lmdb.open(path)
        self.stats = defaultdict(dict)

    def put(self, key, value):
        with self.env.begin(write=True) as txn:
            txn.put(key.encode('utf-8'),
                    json.dumps(value,
                               sort_keys=True,
                               default=json_serial).encode('utf-8'))

    def get(self, key):
        with self.env.begin(write=False) as txn:
            return txn.get(key.encode('utf-8'))
