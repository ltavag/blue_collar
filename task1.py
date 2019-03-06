from task_runner import TaskRunner
from executor import command
import time


@command('hi')
class Task1(TaskRunner):
    """An example task."""

    @TaskRunner.step(status='hi', order=1)
    def fn1(message):
        time.sleep(20) 
        print('hi')

    @TaskRunner.step(status='bye', order=2)
    def fn2(message):
        time.sleep(10) 
        print('bye')

if __name__ == '__main__':
    x = Task1()
    print(x.step.all())
