import sys, os, json, time
from random import randint
import argparse

import parsl
print(parsl.__version__, flush = True)
from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor

from parsl.addresses import address_by_hostname
#from parsl.monitoring.monitoring import MonitoringHub

import parsl_utils

def read_args():
    parser=argparse.ArgumentParser()
    parsed, unknown = parser.parse_known_args()
    for arg in unknown:
        if arg.startswith(("-", "--")):
            parser.add_argument(arg)
    pwargs=vars(parser.parse_args())
    print(pwargs)
    return pwargs

args = read_args()

with open('executors.json', 'r') as f:
    exec_conf = json.load(f)


for label,executor in exec_conf.items():
    for k,v in executor.items():
        if type(v) == str:
            exec_conf[label][k] = os.path.expanduser(v)


# First item in the lists is used in the first attempt (before any retries)
# and the rest of the items are used in the retries (second item is retry 1 and so on)
# - FIXME: If walltime parameter is passed we also need to pass func_name due to this issue in Parsl: https://github.com/Parsl/parsl/issues/2449
RetryHandlerConf = {
    "hello_python_app_1": [
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
            'executor': 'myexecutor_2',
            'args': ['Timeout'],
            'kwargs': {
                'sleep_time': 70,
                'fail': False,
                'func_name': 'hello_python_app_1'
            }
        },
        {
            'executor': 'myexecutor_2',
            'args': ['Success'],
            'kwargs': {
                'sleep_time': 1,
                'fail': False,
                'func_name': 'hello_python_app_1'
            }
        }
    ]
}


def retry_handler(exception, task_record):
    func_name =  task_record['func_name']

    # FIXME: These lines are needed if the walltime parameter is set in the parsl app
    #        because of this issue in Parsl https://github.com/Parsl/parsl/issues/2449
    if func_name == 'wrapper':
        if 'func_name' in task_record['kwargs']:
            func_name = task_record['kwargs']['func_name']
    ############################ end of issue 2449 fix #############################

    print('\nRetrying function {func_name} with fail history:', flush = True)
    print(task_record['fail_history'], flush = True)


    if func_name in RetryHandlerConf:
        print('Retry configuration found for function {func_name}'.format(func_name = func_name), flush = True)
        fRetryHandlerConf = RetryHandlerConf[func_name]
    else:
        print('WARNING: No retry configuration found for function {func_name}!'.format(func_name = func_name), flush = True)
        return 1

    if type(fRetryHandlerConf) == list:
        fail_count = task_record['fail_count']
        print('Fail count: ', fail_count, flush = True)
        # The first item in fRetryHandlerConf is not considered a retry! (-1)
        if  fail_count <= len(fRetryHandlerConf) - 1:
            nfRetryHandlerConf=fRetryHandlerConf[fail_count]
        else:
            print('WARNING: No more retries are defined for this function. Using latest retry!', flush = True)
            nfRetryHandlerConf=fRetryHandlerConf[-1]

        print('Resubmitting with parameters:', flush = True)
        print(json.dumps(nfRetryHandlerConf, indent = 4), flush = True)
        task_record['executor'] = nfRetryHandlerConf['executor']
        task_record['args'] = nfRetryHandlerConf['args']
        task_record['kwargs'] = nfRetryHandlerConf['kwargs']
        return 1
    else:
        print('ERROR: fRetryHandlerConf was expected to be list and is type={rhctype}'.format(rhctype = str(type(fRetryHandlerConf))), flush = True)
        return 99999

    return 1


# PARSL APPS:
# - FIXME: If walltime parameter is passed we also need to pass func_name due to this issue in Parsl: https://github.com/Parsl/parsl/issues/2449
@parsl_utils.parsl_wrappers.log_app
@python_app(executors=[RetryHandlerConf['hello_python_app_1'][0]['executor']])
def hello_python_app_1(name, sleep_time = 10, fail = False, stdout='std.out', stderr = 'std.err', walltime=10, func_name = 'hello_python_app_1'):
    import socket
    from time import sleep
    from datetime import datetime

    f = open('/tmp/time.out', 'a')
    f.write(str(datetime.now()) + '\n')
    f.close()

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



if __name__ == '__main__':
    # Add sandbox directory
    for exec_label, exec_conf_i in exec_conf.items():
        if 'RUN_DIR' in exec_conf_i:
            exec_conf[exec_label]['RUN_DIR'] = os.path.join(exec_conf[exec_label]['RUN_DIR'], args['job_number'])
        else:
            base_dir = '/tmp'
            exec_conf[exec_label]['RUN_DIR'] = os.path.join(base_dir,  args['job_number'])

    print(
        'Executor configuration:',
        json.dumps(exec_conf, indent = 4),
        flush = True
    )

    config = Config(
        retries = 2,
        retry_handler = retry_handler,
        executors = [
            HighThroughputExecutor(
                worker_ports = ((int(exec_conf['myexecutor_1']['WORKER_PORT_1']), int(exec_conf['myexecutor_1']['WORKER_PORT_2']))),
                label = 'myexecutor_1',
                worker_debug = True,             # Default False for shorter logs
                cores_per_worker = float(exec_conf['myexecutor_1']['CORES_PER_WORKER']), # One worker per node
                worker_logdir_root = exec_conf['myexecutor_1']['WORKER_LOGDIR_ROOT'],  #os.getcwd() + '/parsllogs',
                provider = LocalProvider(
                    worker_init = 'source {conda_sh}; conda activate {conda_env}; cd {run_dir}'.format(
                        conda_sh = os.path.join(exec_conf['myexecutor_1']['CONDA_DIR'], 'etc/profile.d/conda.sh'),
                        conda_env = exec_conf['myexecutor_1']['CONDA_ENV'],
                        run_dir = exec_conf['myexecutor_1']['RUN_DIR']
                    ),
                    channel = SSHChannel(
                        hostname = exec_conf['myexecutor_1']['HOST_IP'],
                        username = exec_conf['myexecutor_1']['HOST_USER'],
                        script_dir = exec_conf['myexecutor_1']['SSH_CHANNEL_SCRIPT_DIR'], # Full path to a script dir where generated scripts could be sent to
                        key_filename = '/home/{PW_USER}/.ssh/pw_id_rsa'.format(PW_USER = os.environ['PW_USER'])
                    )
                )
            ),
            HighThroughputExecutor(
                worker_ports = ((int(exec_conf['myexecutor_2']['WORKER_PORT_1']), int(exec_conf['myexecutor_2']['WORKER_PORT_2']))),
                label = 'myexecutor_2',
                worker_debug = True,             # Default False for shorter logs
                cores_per_worker = float(exec_conf['myexecutor_2']['CORES_PER_WORKER']), # One worker per node
                worker_logdir_root = exec_conf['myexecutor_2']['WORKER_LOGDIR_ROOT'],  #os.getcwd() + '/parsllogs',
                provider = LocalProvider(
                    worker_init = 'source {conda_sh}; conda activate {conda_env}; cd {run_dir}'.format(
                        conda_sh = os.path.join(exec_conf['myexecutor_2']['CONDA_DIR'], 'etc/profile.d/conda.sh'),
                        conda_env = exec_conf['myexecutor_2']['CONDA_ENV'],
                        run_dir = exec_conf['myexecutor_2']['RUN_DIR']
                    ),
                    channel = SSHChannel(
                        hostname = exec_conf['myexecutor_2']['HOST_IP'],
                        username = exec_conf['myexecutor_2']['HOST_USER'],
                        script_dir = exec_conf['myexecutor_2']['SSH_CHANNEL_SCRIPT_DIR'], # Full path to a script dir where generated scripts could be sent to
                        key_filename = '/home/{PW_USER}/.ssh/pw_id_rsa'.format(PW_USER = os.environ['PW_USER'])
                    )
                )
            )
        ]
    )

    print('Loading Parsl Config', flush = True)
    parsl.load(config)
    print('Running app', flush = True)
    retry_app_fut = hello_python_app_1(*RetryHandlerConf['hello_python_app_1'][0]['args'], **RetryHandlerConf['hello_python_app_1'][0]['kwargs'])
    print(retry_app_fut.result())