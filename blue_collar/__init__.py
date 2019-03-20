#!/usr/bin/env python
import tornado.httpserver
import tornado.ioloop
import tornado.web
from concurrent.futures import ProcessPoolExecutor
from tornado.escape import (
        json_decode,
)
from tornado import gen, log
import json
from hashlib import sha256
import logging

from .task_store import TaskStore
from .task import Task
from .util import json_serial
from .executor import dispatch

tornado.log.enable_pretty_logging()


class StatsHandler(tornado.web.RequestHandler):
    def get(self):
        """
        Retrieve the entire task body from the task store. 
        This includes metadata such as start time, status 
        and finish time if available.
        """
        all_stats = self.application.task_store.get_stats()
        if all_stats:
            all_stats = json.loads(all_stats)

        for command, stats in all_stats.items():
            for status in stats['status'].keys():
                stats['status'][status] = len(stats['status'][status])

            """Calculate avg time taken."""
            if stats['status'].get('finished', 0) != 0:
                time_taken = float(stats.pop('time_taken'))
                stats['avg_time_taken'] = time_taken / stats['status']['finished']

            """Calculate avg time in queue."""
            all_tasks = sum(stats['status'].values())
            if all_tasks != 0:
                stats['avg_time_in_queue'] = stats.pop('time_in_queue', 0) / float(all_tasks - stats.get('queued', 0))

            """Calculate error rate."""
            done_tasks = sum(stats['status'].get(x, 0) for x in ('error', 'finished'))
            if done_tasks != 0:
                stats['error_rate'] = stats['status'].get('error', 0)  * 100.0 / done_tasks

        self.write(all_stats)


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


def work(port=8888, max_workers=4):
    print(
    """
__________.__                  _________        .__  .__                
\______   \  |  __ __   ____   \_   ___ \  ____ |  | |  | _____ _______ 
 |    |  _/  | |  |  \_/ __ \  /    \  \/ /  _ \|  | |  | \__  \\\\_  __ \\
 |    |   \  |_|  |  /\  ___/  \     \___(  <_> )  |_|  |__/ __ \|  | \/
 |______  /____/____/  \___  >  \______  /\____/|____/____(____  /__|   
        \/                 \/          \/                      \/       

    """)
    application = tornado.web.Application([
        (r"/tasks[/]{0,1}(.*)", TaskHandler),
        (r"/stats", StatsHandler)
    ])

    application.worker_pool = ProcessPoolExecutor(max_workers=max_workers)
    application.task_store = TaskStore()

    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)
    tornado.ioloop.IOLoop.current().start()
