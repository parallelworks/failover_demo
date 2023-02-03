from parsl.app.app import python_app, bash_app
import parsl_utils



# First item in the lists is used in the first attempt (before any retries)
# and the rest of the items are used in the retries (second item is retry 1 and so on)
# - FIXME: If walltime parameter is passed we also need to pass func_name due to this issue in Parsl: https://github.com/Parsl/parsl/issues/2449
RetryHandlerConf = {
    "resilient_app": [
        {
            'executor': 'myexecutor_1',
            'args': ['DivisionByZero'],
            'kwargs': {
                'sleep_time': 70,
                'fail': True,
                'func_name': 'hello_python_app_1'
            }
        },
        {
            'executor': 'myexecutor_1',
            'args': ['Timeout'],
            'kwargs': {
                'sleep_time': 70,
                'fail': False,
                'func_name': 'hello_python_app_1'
            }
        },
        {
            'executor': 'myexecutor_1',
            'args': ['Success'],
            'kwargs': {
                'sleep_time': 1,
                'fail': False,
                'func_name': 'hello_python_app_1'
            }
        }
    ]
}

# PARSL APPS:
@parsl_utils.parsl_wrappers.log_app
@python_app(executors=[RetryHandlerConf['resilient_app'][0]['executor']])
def resilient_app(name, sleep_time = 10, fail = False, stdout='std.out', stderr = 'std.err', walltime=10, func_name = 'resilient_app'):
    """
    App to model the following scenairos:
    1. Failure: if Fail=True), 
    2. Timeout: if sleep_time > walltime
    3. Successul completion
    """
    import socket
    from time import sleep
    from datetime import datetime

    if fail:
        _ = 1/0

    for i in range(sleep_time):
        f = open('/tmp/time.out', 'a')
        f.write(str(i) + '\n')
        f.close()
        time.sleep(1)

    if not name:
        name = 'python_app_1'
    return 'Hello ' + name + ' from ' + socket.gethostname()
