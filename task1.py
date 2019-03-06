import blue_collar
from blue_collar.task_runner import TaskRunner
from blue_collar.executor import command
import time


@command('sleeper')
class Sleeper(TaskRunner):
    """An example task."""

    @TaskRunner.step(status='first_sleep', order=1)
    def fn1(message):
        time.sleep(20) 
        print('Woke up to pee!')

    @TaskRunner.step(status='second_sleep', order=2)
    def fn2(message):
        time.sleep(10) 
        print('It must be morning')

if __name__ == '__main__':
    blue_collar.work()
