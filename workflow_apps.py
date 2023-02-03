from parsl.app.app import python_app, bash_app
import parsl_utils

# PARSL APPS:
@parsl_utils.parsl_wrappers.log_app
@python_app(executors=['myexecutor_1'])
def resilient_app(name, sleep_time = 10, fail = False, walltime = 20, retry_parameters = [], func_name = 'resilient_app'):
    """
    App to model the following scenarios:
    1. Failure: if Fail=True), 
    2. Timeout: if sleep_time > walltime
    3. Successul completion
    """
    import socket
    from time import sleep

    if fail:
        _ = 1/0

    for i in range(sleep_time):
        f = open('/tmp/time.out', 'a')
        f.write(str(i) + '\n')
        f.close()
        sleep(1)

    if not name:
        name = 'resilient_app'
    return 'Hello ' + name + ' from ' + socket.gethostname()
