from functools import wraps
from tornado.escape import json_decode
from tornado.web import HTTPError
from task_store import TaskStore
from datetime import datetime
import time

DB = TaskStore()
COMMAND_MAPPINGS = {}


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

    if task['command'] not in COMMAND_MAPPINGS:
        raise Exception('No such command exists')

    task_runner = COMMAND_MAPPINGS[task['command']]
    for sequence, step in task_runner.step.all().items():

        _, status, step_fn = step

        task['status'] = status
        DB.put(task_id, task)

        step_fn(task['message'])

    task['status'] = 'finished'
    task['finish'] = datetime.now()
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

        COMMAND_MAPPINGS[command_name] = cls
        return wrapper
    return decorator
