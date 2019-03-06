#!/usr/bin/env python
import tornado.httpserver
import tornado.ioloop
import tornado.web
from concurrent.futures import ProcessPoolExecutor
from tornado.escape import (
        json_decode,
)
from tornado import gen
import json
from hashlib import sha256

from task_store import TaskStore
from task import Task
from util import json_serial
from executor import dispatch

import task1



class TaskHandler(tornado.web.RequestHandler):
    def get(self, task_id):
        """
        Retrieve the entire task body from the task store. 
        This includes metadata such as start time, status 
        and finish time if available.
        """
        task = self.application.task_store.get(task_id)
        self.write({
            'id': task_id, 
            'task': json_decode(task)
        })

    @gen.coroutine
    def post(self, _):
        """
        JSON decode the request body. Marshal it in to 
        a task like object so that we can maintain a defined
        structure for the rest of our APIs.
        """
        task = Task(**json_decode(self.request.body))

        """
        Create a deterministic task ID by hashing a combination
        of both the command and the message. This ensures we don't
        re-queue jobs that are already queued.
        """
        task_id = sha256(json.dumps({
            x:task[x]
            for x in ("command", "message")}, sort_keys=True, default=json_serial)
        .encode('utf-8')).hexdigest()

        existing_task = self.application.task_store.get(task_id)
        if existing_task:
            return self.redirect('/tasks/{}'.format(task_id))

        """Add the marshalled task in to our local task store."""
        self.application.task_store.put(task_id, task)

        """Submit the task for execution."""
        worker = self.application.worker_pool.submit(dispatch,
                                                     task_id)

        """Return a redirect to the GET query."""
        return self.redirect('/tasks/{}'.format(task_id))


def main():
    application = tornado.web.Application([
        (r"/tasks[/]{0,1}(.*)", TaskHandler)
    ])

    application.worker_pool = ProcessPoolExecutor(max_workers=4)
    application.task_store = TaskStore()

    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
