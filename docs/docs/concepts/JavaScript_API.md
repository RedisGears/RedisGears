---
title: "JavaScript API"
linkTitle: "JavaScript API"
weight: 2
description: >
    Overview of the JavaScript API
---


## Redis object

* Since version: 2.0.0

The triggers and functions JavaScript API provides a singleton instance of an object named *redis*. The *redis* instance enables registered functions to interact with the Redis server on which they are running. Following is the API provided by the *redis* instance.

### `redis.registerFunction`

* Since version: 2.0.0

Register a new function that can later be invoke using `TFCALL` command.

```JavaScript
//name and callback mandatory
//object with optional arguments
redis.registerFunction(
  'foo', //name
  function(client, args) {}, //callback
  {
    description: 'The description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } // optional arguments
);
```

### `redis.registerAsyncFunction`

* Since version: 2.0.0

Register a new async function that can later be invoke using `TFCALLASYNC` command.

```JavaScript
redis.registerAsyncFunction(
  'foo', //Function name
  function(client, args){}, //callback
  {
    description: 'description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } //optional arguments
);
```

### `redis.registerKeySpaceTrigger`

* Since version: 2.0.0

Register a key space notification trigger that will run whenever a key space notification fired.

```JavaScript
redis.registerKeySpaceTrigger(
  'foo', // trigger name
  'keys:*', //key prefix
  function(client, data) {}, //callback
  {
    description: 'description'
    onTriggerFired: function(client, data){}
  } //optional arguments
)
```

### `redis.registerStreamTrigger`

* Since version: 2.0.0

Register a stream trigger that will be invoke whenever a data is added to a stream.

```JavaScript
redis.registerStreamTrigger(
  'foo', //trigger name
  'stream:*', //prefix
  function(client, data){},//callback
  {
    description: 'Description'
    window: 1,
    isStreamTrimmed: false 
  } //optional arguments
)
```

### `redis.registerClusterFunction`

* Since version: 2.0.0

Register a cluster function that can later be called using `async_client.runOnKey()` or `async_client.runOnShards()`

```JavaScript
redis.registerClusterFunction(
  'foo', //name
  async function(client, ...args){} //callback
)
```

### `redis.config`

* Since version: 2.0.0

```Shell
redis-cli -x RG.FUNCTION LOAD UPGRADE CONFIG '{"example_config": "example_config_value"}' < main.js
```

```JavaScript
var configExample = redis.config.example_config;
```

## Client object

Client object that is used to perform operations on Redis.

### `client.call`

* Since version: 2.0.0

Run a command on Redis. The command is executed on the current Redis instrance.

```JavaScript
client.call(
  '',
  ...args
)
```

### `client.callRaw`

* Since version: 2.0.0

Run a command on Redis but does not perform UTF8 decoding on the result. The command is executed on the current Redis instrance.

```JavaScript
client.callRaw(
  '',
  ...args
)
```

### `client.callAsync`

* Since version: 2.0.0

Call a command on Redis. Allow Redis to block the execution if needed (like `blpop` command) and return the result asynchronously. Returns a promise object that will be resolved when the command invocation finished.

```JavaScript
client.callAsync(
  '',
  ...args
)
```


### `client.callAsyncRaw`

* Since version: 2.0.0

Call a command on Redis. Allow Redis to block the execution if needed (like `blpop` command) and return the result asynchronously. Returns a promise object that will be resolved when the command invocation finished. The command is executed on the current Redis instrance.

```JavaScript
client.callRaw(
  '',
  ...args
)
```


### `client.isBlockAllowed`

* Since version: 2.0.0

Return true if it is allow to return promise from the function callback. In case it is allowed and a promise is return, Redis will wait for the promise to be fulfilled and only then will return the function result.

```JavaScript
client.isBlockAllowed(); // True / False
```

### `client.executeAsync`

* Since version: 2.0.0

Execute the given function asynchroniusly. Return a promise that will be fulfilled when the promise will be resolved/rejected.


```JavaScript
client.executeAsync(
  async function(client){}
)
```



## AsyncClient object

Background client object that is used to perform background operation on Redis.
This client is given to any background task that runs as a JS coroutine.


### `async_client.block`

* Since version: 2.0.0

Blocks Redis for command invocation. All the command that are executed inside the given function is considered atomic and will be wrapped with `Multi/Exec` and send to the replica/AOF.

```JavaScript
async_client.block(
  function(client){}
)
```

### `async_client.runOnKey`

* Since version: 2.0.0

Runs a remote function on a given key. If the key located on the current shard on which we currently runs on, the remote function will run write away. Otherwise the remote function will run on the remote shard. Returns a promise which will be fulfilled when the invocation finishes. Notice that the remote function must return a json serializable result so we can serialize the result back to the original shard. 

Notice that remote function can only perform read operations, not writes are allowed.


```JavaScript
async_client.runOnKey(
  'key1', // key
  'foo', // function name
  ...args
)
```

### `async_client.runOnShards`

* Since version: 2.0.0

Runs a remote function on all the shards. Returns a promise which will be fulfilled when the invocation finishes on all the shards.
 
The result is array of 2 elements, the first is another array of all the results. The second is an array of all the errors happened durring the invocation. Notice that the remote function must return a json serializable result so we can serialize the result back to the original shard.

Notice that remote function can only perform read operations, not writes are allowed.

```JavaScript
async_client.runOnShards(
  'foo', //name
  ...args //arguments
)
```
