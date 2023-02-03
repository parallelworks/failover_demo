import sys, os, json, time
from random import randint
import argparse

import parsl
print(parsl.__version__, flush = True)

import parsl_utils
from parsl_utils.config import config, exec_conf, pwargs, job_number
from parsl_utils.data_provider import PWFile

from workflow_apps import resilient_app

if __name__ == '__main__':
    # Add sandbox directory

    print('\n\nLOADING PARSL CONFIG', flush = True)
    parsl.load(config)

    print('\n\nRUNNING PYTHON APP', flush = True)
    retry_app_fut = resilient_app(
        name = 'DivisionByZero', 
        fail = True,
        retry_params = [
        {
            'executor': 'myexecutor_1',
            'args': ['Timeout'],
            'kwargs': {
                'sleep_time': 70,
                'fail': False,
                'func_name': 'retry_app_fut'
            }
        },
        {
            'executor': 'myexecutor_1',
            'args': ['Success'],
            'kwargs': {
                'sleep_time': 1,
                'fail': False,
                'func_name': 'retry_app_fut'
            }
        }]
    )
    print(retry_app_fut.result())
