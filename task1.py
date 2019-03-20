import blue_collar
from blue_collar.executor import command, step
import time

@command('sleeper')
class Sleeper():
    """An example task."""

    @step(status='first_sleep', order=1)
    def fn1(message):
        time.sleep(2) 
        print('Woke up to pee!')

    @step(status='second_sleep', order=2)
    def fn2(message):
        time.sleep(2) 
        print('It must be morning')


@command('error_sleeper')
class ErrorSleeper():
    """An example task."""

    @step(status='first_sleep', order=1)
    def fn1(message):
        time.sleep(2) 
        print('Woke up to pee!')

    @step(status='second_sleep', order=2)
    def fn2(message):
        time.sleep(2) 
        raise Exception('Oh no, missed my alarm')
