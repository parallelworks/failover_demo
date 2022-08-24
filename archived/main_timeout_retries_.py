import sys, os, json, time
from random import randint
import argparse
import traceback

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

with open('executors.json', 'r') as f:
    exec_conf = json.load(f)


for label,executor in exec_conf.items():
    for k,v in executor.items():
        if type(v) == str:
            exec_conf[label][k] = os.path.expanduser(v)


# PARSL APPS:

# We only need this wrapper to set the executor name!
def hello_python_app_1_wrapper(executor_name):
    @parsl_utils.parsl_wrappers.timeout_app(seconds = 60)
    @parsl_utils.parsl_wrappers.log_app
    @python_app(executors=[executor_name])
    def hello_python_app_1(name = '', sleep_time = 10, fail = False, stdout='std.out', stderr = 'std.err'):
        import socket
        from time import sleep

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

    return hello_python_app_1

class RetryFuture:
    def __init__(self, app_wrapper, executors, *args, **kwargs):
        self.executors = executors
        self.app_wrapper = app_wrapper
        self.app_args = args
        self.app_kwargs = kwargs

        # Submit app (initialize):
        print('\n\nRunning in executor {}'.format(self.executors[0]), flush = True)
        self.fut = self.app_wrapper(executor_name = self.executors[0])(*self.app_args,**self.app_kwargs)

    def result(self):
        for ei,executor in enumerate(self.executors):
            # Get parsl app (define decorator parameters)
            try:
                if ei == 0:
                    return self.fut.result()
                else:
                    print('\n\nRunning in executor {}'.format(executor), flush = True)
                    self.fut = self.app_wrapper(executor_name = executor)(*self.app_args,**self.app_kwargs)
                    return self.fut.result()

            except Exception:
                print(traceback.format_exc(), flush = True)

        raise Exception('App wrapper {} failed in all the executors: {}'.format(self.app_wrapper, ' '.join(self.executors)))


def read_args():
    parser=argparse.ArgumentParser()
    parsed, unknown = parser.parse_known_args()
    for arg in unknown:
        if arg.startswith(("-", "--")):
            parser.add_argument(arg)
    pwargs=vars(parser.parse_args())
    print(pwargs)
    return pwargs

if __name__ == '__main__':
    args = read_args()

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
            )
        ]
    )
    #,
    #    monitoring = MonitoringHub(
    #       hub_address = address_by_hostname(),
    #       resource_monitoring_interval = 5
    #   )
    #)

    print('Loading Parsl Config', flush = True)
    parsl.load(config)

    retry_app_fut = RetryFuture(
        hello_python_app_1_wrapper,
        ['myexecutor_1', 'myexecutor_1'],
        name = '',
        sleep_time = 70,
        fail = False
    )

    print(retry_app_fut.result())