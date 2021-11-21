# RedisAI Integration

[RedisAI](https://oss.redis.com/redisai/) is a Redis module for executing Deep Learning/Machine Learning models and managing their data. Its purpose is being a "workhorse" for model serving, by providing out-of-the-box support for popular DL/ML frameworks and unparalleled performance.
RedisGears has built-in integration with RedisAI via a Python plugin that enables the registration of AI flows, and triggering it upon events.

## Setup
To use RedisAI functionality with RedisGears, both the RedisGears and RedisAI modules should be loaded in the Redis server instance.
The quickest way to try RedisGears with RedisAI is by launching the [`redismod`](https://hub.docker.com/r/redislabs/redismod) Docker container image that bundles together the latest stable releases of Redis and select Redis modules from Redis:

```docker run -p 6379:6379 redislabs/redismod:latest```

Alternatively, you can build RedisAI from its source code by following the instruction [here](https://oss.redis.com/redisai/quickstart/).
Then, you can run the following command to load the two modules (from the RedisGears root directory):

```redis-server --loadmodule ./redisgears.so Plugin ./gears_python.so --loadmodule <path/to/RedisAI-repo>/install-cpu/redisai.so```

## Usage

This integration enabled via the RedisGears' embedded interpreter [Python plugin](runtime.md).
Use `import redisAI` to import RedisAI functionality to the runtime interpreter.

### Objects

The `redisAI` module contains Pythonic wrappers of RedisAI objects (for more information see [here](https://oss.redis.com/redisai/master/)):

* PyTensor - represents a tensor, an n-dimensional array of values

* PyModelRunner - represents a context of model's execution. A model is a computation graph for a supported DL/ML framework backend

* PyScriptRunner - represents a [TorchScript](https://pytorch.org/docs/stable/jit.html) program execution context.
**Note:** The inputs and outputs of a function that will be executed within a script, must be of type `tensor` [RedisAI 1.0 script API](https://oss.redis.com/redisai/commands/#aiscriptrun). Full support for the new [RedisAI 1.2 script API](https://oss.redis.com/redisai/commands/#aiscriptexecute) will be available soon.

* PyDAGRunner - a directional acyclic graph of RedisAI operations (further details below)

Execution requests for models, scripts and DAGs are queued and executed asynchronously.

### Methods

The following sections describe the functional API of the `redisAI` module.

`def createTensorFromValues(type: str, shapes: List[long], values: List[double]) -> PyTensor`

Create a tensor object from values.

* _type_ - the tensor type, can be either "FLOAT", "DOUBLE", "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16" or "BOOL"
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar
* _values_ - the tensor values. The sequence length should match the tensor length (which is determined by the given _shapes_)
* _returns_ - a new PyTensor object

`def createTensorFromBlob(type: str, shapes: List[long], blob: Union[bytearray, bytes]) -> PyTensor`

Create a tensor object from blob.

* _type_ - the tensor type, can be either "FLOAT", "DOUBLE", "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16" or "BOOL"
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar
* _blob_ - the tensor data in binary format. The blob length should match the tensor data size (which is determined by the given _shapes_ and the _type_)
* _returns_ - a new `PyTensor` object

`def setTensorInKey(key: str, tensor: PyTensor) -> None`

Sets a tensor object in Redis keyspace under the given key.

* _key_ - a string that represents the key
* _tensor_ - a `PyTensor` object
* _returns_ - `#!python None`

`def msetTensorsInKeyspace(tensors: Dict[str, PyTensor]) -> None`

Set multiple tensors in Redis keyspace under the given keys.

* _tensors_ - a Python dictionary where the keys are the keys to store in Redis, and the value is the tensor value to store under each key.
* _returns_ - `#!python None`

`def getTensorFromKey(key: str) -> PyTensor`

Get a tensor that is stored in Redis keyspace under the given key.

* _key_ - a string that represents the key
* _returns_ - the `PyTensor` object that is stored in Redis under the given key

`def mgetTensorsFromKeyspace(tensors: List[str]) -> List[PyTensor]`

Get multiple tensors that are stored in Redis keyspace under the given keys.

* _tensors_ - a `List[str]` of keys stored in Redis, each key is holding a value of type tensor
* _returns_ - a `List[PyTensor]` of the tensors stored in Redis under the given keys, respectively

`def tensorToFlatList(tensor: PyTensor) -> Union(List[double], List[long long])`

Get a "flat" list of a tensor's values.

* _tensor_ - a `PyTensor` object
* _returns_ - a `List[double]` of the given tensor values (for floating point tensor types), or `List[long long]` of the tensor values (for integer tensor types)

`def tensorGetDataAsBlob(tensor: PyTensor) -> bytearray`

Get tensor's data in binary form.

* _tensor_ - a `PyTensor` object
* _returns_ - the tensor's underline data as `bytearray`

`def tensorGetDims(tensor: PyTensor) -> Tuple[long]`

Get a tensor's shapes.

* _tensor_ - a `PyTensor` object
* _returns_ - the underline tensor' dimensions

`def createModelRunner(model_key: str) -> PyModelRunner`

Creates a new run context for RedisAI model which is stored in Redis under the given key. To store a model in Redis, one should use the [AI.MODELSTORE command](https://oss.redis.com/redisai/commands/#aimodelstore) before calling this function. This run context is used to hold the required data for the model execution. 

* _model_key_ - a string that represents the model key
* _returns_ - a new `PyModelRunner` object that can be used later on to execute the model over input tensors

`def modelRunnerAddInput(model_runner: PyModelRunner, tensor: PyTensor, name: str) -> long`

Append an input tensor to a model's execution context. The inputs number and order should match the expected order in the underlying model definition.

* _model_runner_ - `PyModelRunner` object that was created using `createModelRunner` call
* _tensor_ - `PyTensor` object that represents the input tensor to append to the model input list
* _name_ - string that represents the input name. Note: the input name can be arbitrary, it should not match any name that is defined in the underline model
* _returns_ - always returns 1

`def modelRunnerAddOutput(model_runner: PyModelRunner, name: str) -> long`

Append a placeholder for an output tensor to return from the model execution. The outputs number and order should match the expected order in the underlying model definition.

* _model_runner_ - `PyModelRunner` object that was created using `createModelRunner` call
* _name_ - string that represents the input name. Note: the input name can be arbitrary, it should not match any name that is defined in the underline model
* _returns_ - always returns 1

`async def modelRunnerRunAsync(model_runner: PyModelRunner) -> List[PyTensor]`

Execute a model in RedisAI based on the given context. The execution is done asynchronously in RedisAI background thread.  

* _model_runner_ - `PyModelRunner` object that was created using `createModelRunner` call, and contains the inputs tensors along with the output placeholders
* _returns_ - a `List[PyTensor]` that contains the outputs of the execution. In case that an error has occurred during the execution, an exception with the appropriate error message will be raised

`def createScriptRunner(script_key: str, function: str) -> PyScriptRunner`

Creates a new run context for RedisAI script (TorchScript). This is stored in Redis under the given key. To store a script in Redis, one should use the [AI.SCRIPTSTORE command](https://oss.redis.com/redisai/commands/#aiscriptstore) prior to calling this function. This run context holds the required data for the script's execution.

* _script_key_ - a string that represents the script key
* _entry_point_ - a string that represents the function to execute within the script
* _returns_ - a new `PyScriptRunner` object that can be used later on to execute the script over input tensors

`def scriptRunnerAddInput(script_runner: PyScriptRunner, tensor: PyTensor, name: str) -> long`

Append an input tensor to a script's execution context. The inputs number and order should match the expected order in the underlying script entrypoint function signature.

* _script_runner_ - a `PyScriptRunner` object that was created using `createScriptRunner` call
* _tensor_ - a `PyTensor` object that represents the input tensor to append to the script input list
* _returns_ - always returns 1

`def scriptRunnerAddInputList(script_runner: PyScriptRunner, tensors: List[PyTensor]) -> long`

Append an input tensor list to a script's execution context. This input should match an expected list of type tensors in the entrypoint function signature.

* _script_runner_ - a `PyScriptRunner` object that was created using `createScriptRunner` call
* _tensors_ - a `List[PyTensor]` that represents the variadic input to append to the script input list
* _returns_ - always returns 1

`def scriptRunnerAddOutput(script_runner: PyScriptRunner, name: str) -> long`

Append a placeholder for an output tensor to return from the script's execution. The outputs number and order should match the expected order in the underlying script's entrypoint function signature.

* _script_runner_ - `PyScriptRunner` object that was created using `createScriptRunner` call
* _returns_ - always returns 1

`async def scriptRunnerRunAsync(script_runner: PyScriptRunner) -> List[PyTensor]`

Execute a RedisAI script (TorchScript) based on the given context. The execution is done asynchronously in RedisAI background thread.

* _script_runner_ - a `PyScriptRunner` object that was created using `createScriptRunner` call, and contains the inputs tensors along with the output placeholders
* _returns_ - a `List[PyTensor]` that contains the outputs of the execution. In case that an error has occurred during the execution, an exception with the appropriate error message will be raised

`def createDAGRunner() -> PyDAGRunner`

Create a new empty `PyDAGRunner` object.

### PyDAGRunner methods

RedisAI can execute a Direct Acyclic Graph (DAG) of operations. For more information and examples about DAGs, refer to the [`AI.DAGEXECUTE` command](https://oss.redis.com/redisai/commands/#aidagexecute).
RedisGears' RedisAI plugin provides access to DAGs via the following methods:

`def DAG.AddInput(tensor: PyTensor, name: str) -> PyDAGRunner`

Append an input tensor to a DAG execution context under the given name. This method is equivalent to using the `LOAD` keyword in the `AI.DAGEXECUTE` command, except that the input tensor, in this case, is given directly, whereas `AI.DAGEXECUTE` command gets the input tensor from the keyspace.

* _name_ - a string that represents the tensor name in the local DAG context (this name will use for specifying the given tensor as input to a DAG operation)
* _tensor_ - a `PyTensor` that represents the input tensor to load into the DAG context
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.TensorSet(tensor: PyTensor, name: str) -> PyDAGRunner`

Append an [`AI.TENSORSET` operation](https://oss.redis.com/redisai/commands/#aitensorset) to the DAG.

* _name_ - a string that represents the tensor name in the local DAG context (this name will use for specifying the given tensor as input to a DAG operation)
* _tensor_ - a `PyTensor` that represents the tensor to load into the DAG context
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.TensorGet(name: str) -> PyDAGRunner`

Append an [`AI.TENSORGET` operation](https://oss.redis.com/redisai/commands/#aitensorget) to the DAG. This call will increase by 1 the number of results that will return from the `DAG.Run()` method.

* _name_ - a string that represents the tensor name in the local DAG context (this name is usually an output of a previous DAG operation)
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.ModelRun(name= : str, inputs= : list[str], outputs= : list[str]) -> PyDAGRunner`

Append an [`AI.MODELEXECUTE` operation](https://oss.redis.com/redisai/commands/#aimodelexecute) to the DAG. Note: this method uses the Python `#!python kwargs` syntax (i.e., arguments names should be specified explicitly).

* _name_ - a string that represents a model to run within the DAG context (the model is assumed to be already stored in Redis under the given name)
* _inputs_ - a `List[str]` of tensors names that were previously loaded to the DAG context (either by `DAG.AddInput()`/`DAG.SetTensor()` methods or as outputs of previous operations). The input tensor names order and length should match the expected inputs of the underline model
* _outputs_ - a `List[str]` of names to be associated with the model output tensors. The output tensors are going to be stored in the DAG local context under these names. The output tensor names order and length should match the expected outputs of the underline model
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.ScriptRun(name= : str, inputs= : list[str], outputs= : list[str]) -> PyDAGRunner`

Append an [`AI.SCRIPTEXECUTE` operation](https://oss.redis.com/redisai/commands/#aiscriptexecute) to the DAG. Note: this method uses the Python `#!python kwargs` syntax (i.e., arguments names should be specified explicitly).

* _name_ - a string that represents a script to run within the DAG context (the script is assumed to be already stored in Redis under the given name)
* _func_ - a string that represents the function to run within the given script (an entry point)  
* _inputs_ - a `List[str]` of names that were previously loaded to the DAG context (either by `DAG.AddInput()`/`DAG.SetTensor()` methods or as outputs of previous operations). The input tensor names order and length should match the expected inputs of the entry point function
* _outputs_ - a `List[str]` of names to be associated with the model output tensors. The output tensors are going to be stored in the DAG local context under these names. The output tensor names order and length should match the expected outputs of the entry point function
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.OpsFromString(ops : str) -> PyDAGRunner`

Append a sequence of operations to the DAG, based on the [`AI.DAGEXECUTE` command](https://oss.redis.com/redisai/commands/#aidagexecute) syntax.

* _ops_ - a string that describes the DAG operations (in particular, should start with `|>`)
* _returns_ - `#!python self` (the calling PyDAGRunner object)

`def DAG.Run() -> List[PyTensor]`

Executes the DAG in RedisAI. Execution is done asynchronously and in parallel when possible.

* _returns_ - a `List[PyTensor]` whose size is the number of `DAG.TensorGet()` operations in the DAG. The `i`-th result in the list corresponds to the output of the `i`-th call to `DAG.TensorGet()`. If an error was detected in the DAG structure or in one of the DAG operations' execution, an exception is raised.

### Examples

The following section shows how to use RedisAI plugin in RedisGears (to reproduce the example results, run them all one after the other):

__Running a basic TensorFlow model__

In our examples, we'll use one of the graphs that RedisAI uses in its tests, namely 'graph.pb', which can be downloaded from [here](https://github.com/RedisAI/RedisAI/raw/master/tests/flow/test_data/graph.pb). This graph was created using TensorFlow with [this script](https://github.com/RedisAI/RedisAI/blob/master/tests/flow/test_data/tf-minimal.py).
To load the model to Redis, we'll use the command line and output pipes:

```
cat graph.pb | redis-cli -x \
               AI.MODELSTORE my_model{1} TF CPU INPUTS 2 a b OUTPUTS 1 c BLOB
```

??? info "Downloading 'graph.pb'"
    Use a web browser or the command line to download 'graph.pb':

    ```
    wget https://github.com/RedisAI/RedisAI/raw/master/tests/flow/test_data/graph.pb
    ```

Next, call `RG.PYEXECUTE` with the following:

```py
import redisAI

async def ModelRun(record):
    tensor_a = redisAI.createTensorFromValues('FLOAT', [2,2], [1.0, 2.0, 3.0, 4.0])
    tensor_b = redisAI.createTensorFromValues('FLOAT', [2,2], [2.0, 3.0, 2.0, 3.0])
    redisAI.msetTensorsInKeyspace({'a{1}': tensor_a, 'b{1}': tensor_b})

    # assuming 'my_model{1}' is a model stored in Redis, receives 2 inputs and returns 1 output
    modelRunner = redisAI.createModelRunner('my_model{1}')     
    redisAI.modelRunnerAddInput(modelRunner, 'a', tensor_a)
    redisAI.modelRunnerAddInput(modelRunner, 'b', tensor_b)
    redisAI.modelRunnerAddOutput(modelRunner, 'c')
    try:
        res = await redisAI.modelRunnerRunAsync(modelRunner)
        redisAI.setTensorInKey('c{1}', res[0])
        return "ModelRun_OK"
    except Exception as e:
        return e

GB("CommandReader").map(ModelRun).register(trigger="ModelRun")
```

Then, you can store the inputs and execute the model by running:

```
redis> RG.TRIGGER ModelRun
1)  "ModelRun_OK"
redis> AI.TENSORGET c{1} VALUES
1) "2"
2) "6"
3) "6"
4) "12"
```

__Running a basic TorchScript__

We can create a RedisAI Script that performs the same computation as the 'graph.pb' model. The script can look like this:

```py
def multiply(tensors: List[Tensor], keys: List[str], args: List[str]):
    return tensors[0] * tensors[1]
```

Assuming that the script is stored in the 'myscript.py' file it can be uploaded via command line and the `AI.SCRIPTSTORE` command as follows:

```
cat myscript.py | redis-cli -x AI.SCRIPTSTORE my_script{1} CPU ENTRY_POINTS 1 multiply SOURCE
```

Next, call `RG.PYEXECUTE` with the following:

```py
import redisAI

async def ScriptRun(record):
    # assuming 'a{1}' and 'b{1}' are tensors stored in Redis
    keys = ['a{1}', 'b{1}']    
    tensors = redisAI.mgetTensorsFromKeyspace(keys)
    
    # assuming 'my_script{1}' is a script stored in Redis that returns 1 output, and `multiply` is one of its entry points.
    scriptRunner = redisAI.createScriptRunner('my_script{1}', 'multiply')     
    redisAI.scriptRunnerAddInput(scriptRunner, tensors[0])
    redisAI.scriptRunnerAddInput(scriptRunner, tensors[1])
    redisAI.scriptRunnerAddOutput(scriptRunner)
    try:
        res = await redisAI.scriptRunnerRunAsync(scriptRunner)
        redisAI.setTensorInKey('c{1}', res[0])
        return "ScriptRun_OK"
    except Exception as e:
        return e

GB("CommandReader").map(ScriptRun).register(trigger="ScriptRun")
```

Then, you can execute the script by running:

```
redis> RG.TRIGGER ScriptRun
1)  "ScriptRun_OK"
redis> AI.TENSORGET c{1} VALUES
1) "2"
2) "6"
3) "6"
4) "12"
```

__Basic DAG examples__

We can create and run simple DAG objects by first calling `RG.PYEXECUTE` with the following:

```py
import redisAI

async def DAGRun_TensorSetTensorGet(record):
    tensor = redisAI.getTensorFromKey('a{1}')
    DAGRunner = redisAI.createDAGRunner()
    DAGRunner.TensorSet('tensor_a', tensor)
    DAGRunner.TensorGet('tensor_a')
    res = await DAGRunner.Run()
    redisAI.setTensorInKey('res1{1}', res[0])
    return "DAG1_OK"

async def DAGRun_ModelRun(record):

    keys = ['a{1}', 'b{1}']
    tensors = redisAI.mgetTensorsFromKeyspace(keys)
    DAGRunner = redisAI.createDAGRunner()
    DAGRunner.Input('tensor_a', tensors[0])
    DAGRunner.Input('tensor_b', tensors[1])
    DAGRunner.ModelRun(name='my_model{1}', inputs=['tensor_a', 'tensor_b'], outputs=['tensor_c'])
    DAGRunner.TensorGet('tensor_c')
    res = await DAGRunner.Run()
    redisAI.setTensorInKey('res2{1}', res[0])
    return "DAG2_OK"

async def DAGRun_ScriptRun(record):

    keys = ['a{1}', 'b{1}']
    tensors = redisAI.mgetTensorsFromKeyspace(keys)
    DAGRunner = redisAI.createDAGRunner()
    DAGRunner.Input('tensor_a', tensors[0])
    DAGRunner.Input('tensor_b', tensors[1])
    DAGRunner.ScriptRun(name='my_script{1}', func='multiply', inputs=['tensor_a', 'tensor_b'], outputs=['tensor_c'])
    DAGRunner.TensorGet('tensor_c')
    res = await DAGRunner.Run()
    redisAI.setTensorInKey('res3{1}', res[0])
    return "DAG3_OK"

async def DAGRun_AddOpsFromString(record):

    keys = ['a{1}', 'b{1}']
    tensors = redisAI.mgetTensorsFromKeyspace(keys)
    DAGRunner = redisAI.createDAGRunner()
    DAGRunner.Input('tensor_a', tensors[0]).Input('tensor_b', tensors[1])
    DAGRunner.OpsFromString('|> AI.MODELEXECUTE my_model{1} INPUTS 2 tensor_a tensor_b OUTPUTS 1 tensor_c |> AI.TENSORGET tensor_c')
    res = await DAGRunner.Run()
    redisAI.setTensorInKey('res4{1}', res[0])
    return "DAG4_OK"

GB("CommandReader").map(DAGRun_TensorSetTensorGet).register(trigger="DAGRun1")
GB("CommandReader").map(DAGRun_ModelRun).register(trigger="DAGRun2")
GB("CommandReader").map(DAGRun_ScriptRun).register(trigger="DAGRun3")
GB("CommandReader").map(DAGRun_AddOpsFromString).register(trigger="DAGRun4")
```

Then, you can execute these DAG examples one by one, by running:

```
redis> RG.TRIGGER DAGRun1
1)  "DAG1_OK"
redis> AI.TENSORGET res1{1} VALUES
1) "1"
2) "2"
3) "3"
4) "4"
redis> RG.TRIGGER DAGRun2
1)  "DAG2_OK"
redis> AI.TENSORGET res2{1} VALUES
1) "2"
2) "6"
3) "6"
4) "12"
redis> RG.TRIGGER DAGRun3
1)  "DAG3_OK"
redis> AI.TENSORGET res3{1} VALUES
1) "2"
2) "6"
3) "6"
4) "12"
redis> RG.TRIGGER DAGRun4
1)  "DAG4_OK"
redis> AI.TENSORGET res4{1} VALUES
1) "2"
2) "6"
3) "6"
4) "12"
```

__Error handling example__

The following example shows how to catch an error that has occurred during an execution. We first call `RG.PYEXECUTE` with the following:

```py
import redisAI

async def DAGRun_ScriptRunError(record):

    keys = ['a{1}', 'b{1}']
    tensors = redisAI.mgetTensorsFromKeyspace(keys)
    DAGRunner = redisAI.createDAGRunner()
    DAGRunner.Input('tensor_a', tensors[0])
    DAGRunner.Input('tensor_b', tensors[1])
    DAGRunner.ScriptRun(name='my_script{1}', func='no_func', inputs=['tensor_a', 'tensor_b'], outputs=['tensor_c'])
    DAGRunner.TensorGet('tensor_c')
    try:
        res = await DAGRunner.Run()
    except Exception as e:
        return e

GB("CommandReader").map(DAGRun_ScriptRunError).register(trigger="DAGRun_Error")

```

Then, you can execute the following DAG which contains an error, and catch the error, by running:

```
redis> RG.TRIGGER DAGRun_Error
1)  "Function does not exist: no_func"
```
