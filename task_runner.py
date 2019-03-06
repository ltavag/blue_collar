from functools import wraps

class TaskRunner:
    """Base class for all Task Runners."""
    class step():
        """
        A decorator in the form of a class. The goal 
        of this class is to create an expressive syntax
        such that a task can be broken in to multiple
        pieces and status' can be defined for each of
        those pieces.
        """
        _all_subtasks = {}

        @classmethod
        def add_step(self, step):
            order, status, fn = step
            if order in self._all_subtasks:
                raise Exception('Multiple subtasks have the same `order`')

            self._all_subtasks[order] = step

        @classmethod
        def all(self):
            return self._all_subtasks

        def __init__(self, status='running', order=0):
            self.status = status
            self.order = order


        def __call__(self, fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                fn(*args, **kwargs)

            self.add_step((self.order, self.status, fn))
            return wrapper

