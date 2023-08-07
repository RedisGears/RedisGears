---
title: "Cluster Support"
linkTitle: "Cluster Support"
weight: 4
description: >
    Cluster support for triggers and functions
---

**Notice**: On an OSS cluster, before executing any gears function, you must send a `REDISGEARS_2.REFRESHCLUSTER` command to all the shards so that they will be aware of the cluster topology. Without this step, each shard will act as a single OSS instance.

Triggers and functions support cross-shard operation on Redis clusters. This means that it is possible to call a function that will be invoked on another shard. We call such a function a remote function.

Just like local functions, remote function must be declared on library load time using `redis.registerClusterFunction` API. The following example declares a remote function that returns the number of keys on the shard:

```js
redis.registerClusterFunction("dbsize", async(async_client) => {
    return client.block((async_client) => {
        return client.call("dbsize").toString();
    });
});
```

`redis.registerClusterFunction` is passed the remote function name, which will be used later to call the remote function, and the remote function code. The remote function must be a Coroutine (async function) and it is executed in the background on the remote shard. For more information about async function, please refer to [Sync and Async Run](/docs/interact/programmability/triggers-and-functions/concepts/sync_async/) page.

We have couple of options for calling a remote function. These options are exposed through the async client that is given to a Coroutine:

* `async_client.runOnShards` - run the remote function on all the shards (including the current shard). Returns a promise that, once resolved, will give two nested arrays, the first contains another array with the results from all the shards and the other contains an array of errors (`[[res1, res2, ...],[err1, err2, ..]]`).
* `async_client.runOnKey` - run the remote function on the shard responsible for a given key. Returns a promise that, once resolved, will give the result from the remote function execution or raise an exception in the case of an error.

The following example registers a function that will return the total number of keys on the cluster. The function will use the remote function defined above:

```js
redis.registerAsyncFunction("my_dbsize", async(async_client) => {
    let res = await async_client.runOnShards("dbsize");
    let results = res[0];
    let errors = res[1];
    if (errors.length > 0) {
        return errors;
    }
    let sum = BigInt(0);
    results.forEach((element) => sum+=BigInt(element));
    return sum;
});
```

First, the function executes a remote function on all shards that returns the number of keys on each shard, then the function adds all the results and returns total number of keys for all shards.

Here's the complete example:

```js
#!js name=lib api_version=1.0

redis.registerClusterFunction("dbsize", async(client) => {
    return client.block((client) => {
        return client.call("dbsize").toString();
    });
});

redis.registerAsyncFunction("test", async(async_client) => {
    let res = await async_client.runOnShards("dbsize");
    let results = res[0];
    let errors = res[1];
    if (errors.length > 0) {
        return errors;
    }
    let sum = BigInt(0);
    results.forEach((element) => sum+=BigInt(element));
    return sum;
});
```

## Arguments and Results Serialization

It is possible to pass arguments to the remote function. The arguments will be given to the remote function after the `async_client`. The following example shows how to get a value of a key from any shard in the cluster:

```js
#!js name=lib api_version=1.0
const remote_get = "remote_get";

redis.registerClusterFunction(remote_get, async(client, key) => {
    let res = client.block((client) => {
        return client.call("get", key);
    });
    return res;
});

redis.registerAsyncFunction("test", async (async_client) => {
    return await async_client.runOnKey("x", remote_get, "x");
});
```

The function `test` will return the value of `x` regardless of which shard the function was executed on.

The remote function arguments and results are serialized in the following way:

1. If the argument is of type `ArrayBuffer`, the data will be sent as is.
2. Otherwise, RedisGears will try to serialize the give arguments (or the return value) as JSON using `JSON.stringify`. A serialization failure will cause an error to be raised.

## Execution Timeout

Remote functions will not be permitted to run forever and will timeout. The timeout period can be configured using [remote-task-default-timeout](/docs/interact/programmability/triggers-and-functions/configuration/#remote-task-default-timeout). When using `async_client.runOnShards` API, the timeout will be added as error to the error array. When using `async_client.runOnKey`, a timeout will cause an exception to be raised.

## Remote Function Limitations

All the limitations listed on [Coroutine](/docs/interact/programmability/triggers-and-functions/concepts/sync_async/) also apply to remote functions. Remote function also come with some extra limitations:

* Remote functions can only perform read operations. An attempt to perform a write operation will result in an error.
* Remote function are not guaranteed to succeed (if the shard crashed for example). In such cases a timeout error will be given.
