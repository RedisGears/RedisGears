# RedisAI Integration

[RedisAI](https://oss.redis.com/redisai/) RedisAI is a Redis module for executing Deep Learning/Machine Learning models and managing their data. Its purpose is being a "workhorse" for model serving, by providing out-of-the-box support for popular DL/ML frameworks and unparalleled performance.
RedisGears has a built-in integration with RedisAI via Python plugin that enables the registration of AI flows, and triggering it upon events.

## Setup
To use RedisAI functionality with RedisGears, RedisAI module should be loaded to the Redis server along with RedisGears.
The quickest way to try RedisGears with RedisAI is by launching `redismod` Docker container image that bundles together the latest stable releases of Redis and select Redis modules from Redis:

```docker run -p 6379:6379 redislabs/redismod:latest```

Alternatively, you can build RedisAI from its source code by following the instruction [here](https://oss.redis.com/redisai/quickstart/).
Then, you can run the following command to load the two modules (from RedisGears root directory):

```redis-server --loadmodule ./redisgears.so Plugin gears_pyhton.so --loadmodule <path/to/RedisAI-repo>/install-cpu/redisai.so```

## Usage

The integration with RedisAI is enabled via RedisGears' embedded python interpreter [python plugin](runtime.md).
Use `import redisAI` to import RedisAI functionality to the runtime interpreter.

### Objects

`redisAI` module contains pythonic representations of RedisAI objects (further explanations about these objects is available in RedisAI [docs](https://oss.redis.com/redisai/master/)):
* PyTensor - represents a tensor - an n-dimensional array of values
* PyModel - represents a model - computation graph by one of the supported DL/ML framework backends
* PyScript - represents a [TorchScript](https://pytorch.org/docs/stable/jit.html) program
* PyDAG - directional acyclic graph of RedisAI operations (further details below)

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

sets a tensor object in Redis keyspace under the given key. This will acquire Redis GIL as it access the keyspace 
* _key_ - string that represents the key. 
* _tensor_ - PyTensor object that was created with `createTensorFromBlob` or `createTensorFromValue`.
* _returns_ - None

`def msetTensorsInKeyspace(tensors: dict[str, PyTensor])`

sets multiple tensors in Redis keyspace under the given keys. This will acquire Redis GIL as it access the keyspace
* _tensors_ - dictionary where the keys are the keys to store in Redis, and the value is the tensor value to store under each key
* _returns_ - None