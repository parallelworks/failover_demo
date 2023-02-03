# Failover Demo
This workflow is a "hello world" failover example using Parsl and the [parsl_utils](https://github.com/parallelworks/parsl_utils) repository for integration with the PW platform. An app is re-launched after failure using the retries and retry_handler parameters in the Parsl config. 

## Defining Failovers
1. Define the `parsl_retries` in the workflow's input form
2. Define the keyworded argument `retry_parameters` in the Parsl app
3. Call the Parsl app with the corresponding retry parameters 

For example, the following app takes a keyworded argument named `retry_parameters`:
```
@python_app(executors=['myexecutor_1'])
def resilient_app(name, sleep_time = 10, fail = False, walltime = 20, retry_parameters = [], func_name = 'resilient_app'):
```

The retry parameters are specified when the app is called:
```
resilient_app_fut = resilient_app(
        'DivisionByZero', 
        fail = True,
        retry_parameters = [
        {
            'executor': 'myexecutor_1',
            'args': ['Timeout'],
            'kwargs': {
                'sleep_time': 70,
                'fail': False,
                'func_name': 'resilient_app'
            }
        },
        {
            'executor': 'myexecutor_1',
            'args': ['Success'],
            'kwargs': {
                'sleep_time': 1,
                'fail': False,
                'func_name': 'resilient_app'
            }
        }]
    )
 
```

The `retry_parameters` argument is a list that specifies the task parameters to overwite at each retry. Note that the first item in the list corresponds to the first retry, the second item to the second retry and so on. If the number of retries is larger than the number of items in the list or if no `retry_parameters` argument is defined the task parameters are not overwritten and the task is resubmitted with the same parameters. Every item in the list may contain the following **optional** parameters:
1. **executor**: Resubmit the task to a different executor
2. **args**: Overwrite the non-keyworded arguments of the task. Note that all non-keyworded arguments must be specified here.
3. **kwargs**: Overwrite the specified keyworded arguments of the task. Note that is is not necessary to specify all the keyworded arguments, only those to overwrite.

Note that in the example above specifying the executor again in the first and second retry is not necessary. Similarly, there is no need to set the `fail` keyworded argument to `False` in the second retry. 

## Github
This workflow clones the [failover_demo](https://github.com/parallelworks/failover_demo) public github repository at runtime. 
