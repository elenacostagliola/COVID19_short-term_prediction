# Credits: https://gist.github.com/aaronchall/6331661fe0185c30a0b4

import sys
import threading
from time import sleep

try:
    import thread
except ImportError:
    import _thread as thread


def function_execution_timeout(fn_name):
    # print to stderr, unbuffered in Python 2.
    print('Timeout: execution of function {0} took too long'.format(fn_name), file=sys.stderr)
    sys.stderr.flush()  # Python 3 stderr is likely buffered.
    raise TimeoutError
    # thread.interrupt_main()  # raises KeyboardInterrupt


def exit_after(s):
    """
    use as decorator to exit process if
    function takes longer than s seconds
    """

    def outer(fn):
        def inner(*args, **kwargs):
            timer = threading.Timer(s, function_execution_timeout, args=[fn.__name__])
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result

        return inner

    return outer


# Test function
@exit_after(5)
def countdown(n):
    print('countdown started', flush=True)
    for i in range(n, -1, -1):
        print(i, end=', ', flush=True)
        sleep(1)
    print('countdown finished')


# Test function
def run_countdown():
    try:
        countdown(5)
    except KeyboardInterrupt:
        print("Got KeyboardInterrupt")
