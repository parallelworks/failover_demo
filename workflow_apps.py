from parsl.app.app import python_app, bash_app
import parsl_utils

# PARSL APPS:
@parsl_utils.parsl_wrappers.log_app
@python_app(executors=['myexecutor_1'])
def resilient_app(name, sleep_time = 10, fail = False, walltime = 20, retry_parameters = None, func_name = 'resilient_app'):
    """
    App to model the following scenarios:
    1. Failure: if Fail=True), 
    2. Timeout: if sleep_time > walltime
    3. Successul completion
    """
    import socket
    from time import sleep

    import logging
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    
    def get_logger(log_file, name, level = logging.INFO):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logging.getLogger(name)

    logger = get_logger('resilient_app.log', 'resilient_app')

    if fail:
        _ = 1/0

    for i in range(sleep_time):
        logger.info('Waiting ' + str(i))
        sleep(1)

    if not name:
        name = 'resilient_app'
    return 'Hello ' + name + ' from ' + socket.gethostname()
