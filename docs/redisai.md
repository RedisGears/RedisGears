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
* Tensor - represents an n-dimensional array of values
* Model - represents a computation graph by one of the supported DL/ML framework backends
* Script - represents a [TorchScript](https://pytorch.org/docs/stable/jit.html) program
* DAG - directional acyclic graph of operations (further details below)

Execution requests for models, scripts and DAGs are queued and executed asynchronously.

### Methods

The following sections describe the functional API of `redisAI` module.

`def createTensorFromValues(Str: type, List[Long]: shapes, List[Double]: values)`

create a tensor object from values.
* _type_ - the tensor type, can be either FLOAT, DOUBLE, INT8, INT16, INT32, INT64, UINT8, UINT16 or BOOL.
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar.
* _values_ - the tensor values. The sequence length should match the tensor length (which is determined by the given _shapes_) 
* _returns_ - A PyTensor object.

`def createTensorFromBlob(Str: type, List[Long]: shapes, Union[bytearray, bytes]: blob)`

create a tensor object from values.
* _type_ - the tensor type, can be either FLOAT, DOUBLE, INT8, INT16, INT32, INT64, UINT8, UINT16 or BOOL.
* _shapes_ - the tensor dimensions. If empty, the tensor is considered to be a scalar.
* _blob_ - the tensor data in binary format. The blob length should match the tensor data size (which is determined by the given _shapes_ and the _type_)
* _returns_ - A PyTensor object.

