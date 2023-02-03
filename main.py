import sys, os, json, time
from random import randint
import argparse

import parsl
print(parsl.__version__, flush = True)

import parsl_utils
from parsl_utils.config import config, exec_conf, pwargs, job_number
from parsl_utils.data_provider import PWFile

from workflow_apps import resilient_app, RetryHandlerConf

# FIXME: This function may need to move to parsl_utils
def retry_handler(exception, task_record):
    func_name =  task_record['func_name']

    # FIXME: These lines are needed if the walltime parameter is set in the parsl app
    #        because of this issue in Parsl https://github.com/Parsl/parsl/issues/2449
    if func_name == 'wrapper':
        if 'func_name' in task_record['kwargs']:
            func_name = task_record['kwargs']['func_name']
    ############################ end of issue 2449 fix #############################
    print('\nRetrying function {}'.format(func_name), flush = True)
    print('Fail history:', flush = True)
    print(task_record['fail_history'], flush = True)
    print('Resource session messages:', flush = True)
    rmsgs = parsl_utils.resource_info.get_resource_messages(exec_conf[task_record['executor']]['POOL'])
    [ print(msg, flush = True) for msg in rmsgs ]

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




if __name__ == '__main__':
    # Add sandbox directory

    print('\n\nLOADING PARSL CONFIG', flush = True)
    parsl.load(config)

    print('\n\nRUNNING PYTHON APP', flush = True)
    retry_app_fut = resilient_app(
        *RetryHandlerConf['resilient_app'][0]['args'],
        **RetryHandlerConf['resilient_app'][0]['kwargs']
    )
    print(retry_app_fut.result())
