# RedisAI Integration

[RedisAI](https://oss.redis.com/redisai/) RedisAI is a Redis module for executing Deep Learning/Machine Learning models and managing their data. Its purpose is being a "workhorse" for model serving, by providing out-of-the-box support for popular DL/ML frameworks and unparalleled performance.
RedisGears has a built-in integration with RedisAI via Python plugin that enables the registration of AI flows, and triggering it upon events.

## Setup
To use RedisAI functionality with RedisGears, RedisAI module should be loaded to the Redis server along with RedisGears.
The quickest way to try RedisGears with RedisAI is by launching `redismod` Docker container image that bundles together the latest stable releases of Redis and select Redis modules from Redis:

```docker run -p 6379:6379 redislabs/redismod:latest```

Alternatively, you can build RedisAI from its source code by following the instruction [here](https://oss.redis.com/redisai/quickstart/).
Then, you can run the following command to load the two modules (from RedisGears root directory):

```redis-server --loadmodule ./redisgears.so Plugin ./gears_python.so --loadmodule <path/to/RedisAI-repo>/install-cpu/redisai.so```

## Usage

The integration with RedisAI is enabled via RedisGears' embedded python interpreter [python plugin](runtime.md).
Use `import redisAI` to import RedisAI functionality to the runtime interpreter.

### Objects

`redisAI` module contains pythonic wrappers of RedisAI objects (further explanations about these objects is available in RedisAI [docs](https://oss.redis.com/redisai/master/)):
* PyTensor - represents a tensor - an n-dimensional array of values
* PyModelRunner - represents a context of model execution. A model in RedisAI is a computation graph by one of the supported DL/ML framework backends
* PyScriptRunner - represents a context of [TorchScript](https://pytorch.org/docs/stable/jit.html) program execution.
  **Note:** RedisGears currently supports deprecated API in which the inputs to each entry point function within the script are tensors or a list of tensors only.

* PyDAGRunner - directional acyclic graph of RedisAI operations (further details below)

Execution requests for models, scripts and DAGs are queued and executed asynchronously.

### Methods

The following sections describe the functional API of `redisAI` module.

`def createTensorFromValues(type: str, shapes: list[long], values: list[double])`

create a tensor object from values.
* _type_ - the tensor type, can be either FLOAT, DOUBLE, INT8, INT16, INT32, INT64, UINT8, UINT16 or BOOL.
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar.
* _values_ - the tensor values. The sequence length should match the tensor length (which is determined by the given _shapes_) 
* _returns_ - A new PyTensor object.

`def createTensorFromBlob(type: str, shapes: list[long], blob: Union[bytearray, bytes])`

create a tensor object from values.
* _type_ - the tensor type, can be either FLOAT, DOUBLE, INT8, INT16, INT32, INT64, UINT8, UINT16 or BOOL.
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar.
* _blob_ - the tensor data in binary format. The blob length should match the tensor data size (which is determined by the given _shapes_ and the _type_)
* _returns_ - A new PyTensor object.

`def setTensorInKey(key: str, tensor: PyTensor)`

sets a tensor object in Redis keyspace under the given key. The operation will acquire Redis GIL as it access the keyspace 
* _key_ - string that represents the key. 
* _tensor_ - PyTensor object that was created with `createTensorFromBlob` or `createTensorFromValue`.
* _returns_ - None

`def msetTensorsInKeyspace(tensors: dict[str, PyTensor])`

sets multiple tensors in Redis keyspace under the given keys. The operation will acquire Redis GIL as it access the keyspace.
* _tensors_ - dictionary where the keys are the keys to store in Redis, and the value is the tensor value to store under each key
* _returns_ - None

`def getTensorFromKey(key: str)`

get a tensor that is stored in Redis keyspace under the given key. The operation will acquire Redis GIL as it access the keyspace
* _key_ - string that represents the key.
* _returns_ - the PyTensor object that is stored in Redis under the given key. 

`def mgetTensorsFromKeyspace(tensors: list[str])`

get multiple tensors that are stored in Redis keyspace under the given keys. The operation will acquire Redis GIL as it access the keyspace.
* _tensors_ - list of strings that are associated with keys stored in Redis, each key is holding a value of type tensor.
* _returns_ - list of PyTensor objects that correspond to the tensors stored in Redis under the given keys, respectively. 

`def tensorToFlatList(tensor: PyTensor)`

get a "flat" list of a tensor's values
* _tensor_ - PyTensor object.
* _returns_ - list of the given tensor values

`def tensorGetDataAsBlob(tensor: PyTensor)`

get tensor's data in binary form
* _tensor_ - PyTensor object.
* _returns_ - the tensor's underline data as byte array

`def tensorGetDims(tensor: PyTensor)`

get tensor's shapes
* _tensor_ - PyTensor object.
* _returns_ - a tuple of the underline tensor' dimensions.

`def createModelRunner(model_key: str)`

creates a new run context for RedisAI model which is stored in Redis under the given key. To store a model in Redis, one should use the [AI.MODELSTORE command](https://oss.redis.com/redisai/commands/#aimodelstore) before calling this function. This run context is used to hold the required data for the model execution. 
* _model_key_ - string that represents the model key.
* _returns_ - A new PyModelRunner object that can be used later on to execute the model over input tensors

`def modelRunnerAddInput(model_runner: PyModelRunner, tensor: PyTensor, name: str)`

* Append an input tensor to a model execution context. The inputs number and order should match the expected order in the underline model definition.
* _model_runner_ - PyModelRunner object that was created using `createModelRunner` call.
* _tensor_ - PyTensor object that represents the input tensor to append to the model input list.
* _name_ - string that represents the input name. Note: the input name can be arbitrary, it should not match any name that is defined in the underline model.
* _returns_ - always returns 1

`def modelRunnerAddOutput(model_runner: PyModelRunner, name: str)`

* Append a placeholder for an output tensor to return from the model execution. The outputs number and order should match the expected order in the underline model definition.
* _model_runner_ - PyModelRunner object that was created using `createModelRunner` call.
* _name_ - string that represents the input name. Note: the input name can be arbitrary, it should not match any name that is defined in the underline model.
* _returns_ - always returns 1

`async def modelRunnerRunAsync(model_runner: PyModelRunner)`

* Performs an execution of a model in RedisAI based on the given context. The execution is done asynchronously in RedisAI background thread.  
* _model_runner_ - PyModelRunner object that was created using `createModelRunner` call, and contains the inputs tensors along with the output placeholders. 
* _returns_ - a list of PyTensor objects that contains the outputs of the execution. In case that an error has occurred during the execution, an exception with the appropriate error message will be raised.  

`def createScriptRunner(script_key: str, function: str)`

creates a new run context for RedisAI script (torch script) which is stored in Redis under the given key. To store a script in Redis, one should use the [AI.SCRIPTSTORE command](https://oss.redis.com/redisai/commands/#aiscriptstore) before calling this function. This run context is used to hold the required data for the script execution.
* _script_key_ - string that represents the script key.
* _entry_point_ - string that represents the function to execute within the script.
* _returns_ - A new PyScriptRunner object that can be used later on to execute the script over input tensors

`def scriptRunnerAddInput(script_runner: PyScriptRunner, tensor: PyTensor, name: str)`

* Append an input tensor to a script execution context. The inputs number and order should match the expected order in the underline script entry point function signature.
* _script_runner_ - PyScriptRunner object that was created using `createScriptRunner` call.
* _tensor_ - PyTensor object that represents the input tensor to append to the script input list.
* _returns_ - always returns 1

`def scriptRunnerAddInputList(script_runner: PyScriptRunner, tensors: list[PyTensor])`

* Append an input tensor list to a script execution context. This input should match an expected list of type tensors in the entry point function signature.
* _script_runner_ - PyScriptRunner object that was created using `createScriptRunner` call.
* _tensors_ - a list of PyTensor objects that represents the variadic input to append to the script input list.
* _returns_ - always returns 1
* 
`def scriptRunnerAddOutput(script_runner: PyScriptRunner, name: str)`

* Append a placeholder for an output tensor to return from the script execution. The outputs number and order should match the expected order in underline script entry point function signature.
* _script_runner_ - PyScriptRunner object that was created using `createScriptRunner` call.
* _returns_ - always returns 1

`async def scriptRunnerRunAsync(script_runner: PyScriptRunner)`

* Performs an execution of a torch script in RedisAI based on the given context. The execution is done asynchronously in RedisAI background thread.
* _script_runner_ - PyScriptRunner object that was created using `createScriptRunner` call, and contains the inputs tensors along with the output placeholders.
* _returns_ - a list of PyTensor objects that contains the outputs of the execution. In case that an error has occurred during the execution, an exception with the appropriate error message will be raised.  
