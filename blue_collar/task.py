from tornado.web import HTTPError
from datetime import datetime

class Task(dict):
    """An easily serializable, extendable class that represents a marshaled task."""
    def __init__(self, **kwargs):
        """Enforce some basic keys in the task object.

        command -- The name of the command for the specific task. 
        message -- The payload required for the task.
        start   -- The start time for the task 
        finish  -- The time that the task finished.
        status  -- The current state of the task.
        """
        if 'command' not in kwargs:
            raise HTTPError(422, 'No command specified')

        if 'message' not in kwargs:
            raise HTTPError(422, 'No message specified')

        self['start'] = datetime.now()
        self['finish'] = None
        self['status'] = 'queued'
        self.update(kwargs)
