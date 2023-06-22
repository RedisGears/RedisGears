---
title: "JavaScript API"
linkTitle: "JavaScript API"
weight: 2
description: >
    Overview of the JavaScript API
---


## Redis object

* Since version: 2.0.0

The Triggers and Functions JavaScript API always provides a singleton instance of an object named *redis*. The *redis* instance enables the functions to interact with the Redis server that is running it. Following is the API provided by the *redis* object instance.

### `redis.registerFunction`

* Since version: 2.0.0

The `redis.registerFunction()` registers a User Function that can be called with `TFCALL`.

```JavaScript
// Object with all arguments
redis.registerFunction({
  name: 'foo', 
  description: 'description', 
  flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM],
  callback: function(client, args){}
});

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

```JavaScript
// Object with all mandatory and optional arguments
redis.registerAsyncFunction({
  name: 'foo', 
  description: 'description',
  flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM],
  callback: function(client, args){}
});

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

```JavaScript
redis.registerKeySpaceTrigger({
  name: 'foo', 
  description: 'description',
  prefix: 'keys:*',
  eventTypes: ['hset'],
  keyTypes: ['hash'],
  callback: function(client, data){},
  onTriggerFired: function(client, data){}
})

redis.registerKeySpaceTrigger(
  'foo', // trigger name
  'keys:*', //key prefix
  function(client, data) {}, //callback
  {
    description: 'description'
    eventTypes: ['hset'],
    keyTypes: ['hash'],
    onTriggerFired: function(client, data){}
  } //optional arguments
)
```

### `redis.registerStreamTrigger`

* Since version: 2.0.0

```JavaScript
redis.registerStreamTrigger({
  name: 'foo',
  description: '',
  prefix: 'stream1',
  window: 1,
  isStreamTrimmed: false,
  callback: function(client, data){}
});

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

### `client.call`

* Since version: 2.0.0

```JavaScript
client.call(
  '',
  ...args
)
```

### `client.callRaw`

* Since version: 2.0.0

```JavaScript
client.callRaw(
  '',
  ...args
)
```

### `client.block`

* Since version: 2.0.0

```JavaScript
client.block(
  function(client){}
)
```

### `client.isBlockAllowed`

* Since version: 2.0.0

```JavaScript
client.isBlockAllowed(); // True / False
```

### `client.executeAsync`

* Since version: 2.0.0

```JavaScript
client.executeAsync(
  async function(client){}
)
```

### client.runOnShards

* Since version: 2.0.0

```JavaScript
client.runOnShards(
  'foo', //name
  ...args //arguments
)
```

### client.runOnKey

* Since version: 2.0.0

```JavaScript
client.runOnKey(
  'key1', // key
  'foo', // function name
  ...args
)
```
