from functools import wraps
from collections import defaultdict
import inspect

from tornado.escape import json_decode
from tornado.web import HTTPError
from .task_store import TaskStore
from datetime import datetime
import time

DB = TaskStore()
COMMAND_MAPPINGS = {}
TASK_STRUCTURE = defaultdict(dict)


def dispatch(task_id):
    """Dispatch the command to the correct task runner.

    This function is the essentially a route handler that
    maps the incoming commands to the appropriate task runner
    classes. It is critical that there are no uncaught exceptions
    that result from this function as it is running from a separate
    process, from which errors are hard to recover from.

    * Load the task body from LMDB
    * Check to see that there is an implemented task runner for the
      command class
    * Run each step of the task runner in sequential order, ensuring
      that the status is updated in between each step
    * Once the task runner has executed all steps, set the status to
      finished, and set the finish time.
    
    """
    task = DB.get(task_id)
    task = json_decode(task)

    try:
        if task['command'] not in COMMAND_MAPPINGS:
            raise Exception('No such command exists')

        task_cls = COMMAND_MAPPINGS[task['command']]
        for sequence, step in TASK_STRUCTURE[task_cls].items():

            _, status, step_fn = step

            task['status'] = status
            DB.put(task_id, task)

            step_fn(task['message'])

        task['status'] = 'finished'
        task['finish'] = datetime.now()

    except Exception as e:
        task['error'] = {
            'step': task['status'],
            'reason': f'{ e.__class__.__name__ }: { e.args[0] }'
        }

        task['status'] = 'error'

    finally:
        DB.put(task_id, task)


def command(command_name):
    """
    A decorator to help map the names of the
    commands to the appropriate classes which
    contains steps for their execution.
    """
    def decorator(cls):
        @wraps(cls)
        def wrapper(*args, **kwargs):
            return cls(*args, **kwargs)

        COMMAND_MAPPINGS[command_name] = cls.__name__
        return wrapper
    return decorator


class step():
    """
    A decorator in the form of a class. The goal 
    of this class is to create an expressive syntax
    such that a task can be broken in to multiple
    pieces and status' can be defined for each of
    those pieces.
    """
    def add_step(self, step):
        order, status, fn = step
        if order in TASK_STRUCTURE[self.class_name]:
            raise Exception('Multiple subtasks have the same `order`')

        TASK_STRUCTURE[self.class_name][order] = step

    def __init__(self, status='running', order=0):
        self.class_name = inspect.currentframe().f_back.f_code.co_name
        self.status = status
        self.order = order


    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            fn(*args, **kwargs)

        self.add_step((self.order, self.status, fn))
        return wrapper
