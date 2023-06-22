---
title: "Cluster Support"
linkTitle: "Cluster Support"
weight: 4
description: >
    Cluster support for Triggers and Functiosn
---

**Notice** on oss cluster, before executing any gears function, you must send `REDISGEARS_2.REFRESHCLUSTER` command to all the shards so that all the shards will be aware of the cluster topology. Without this step, each shard will act as a single oss instance.

Triggers and Functions support cross shard operation on Redis cluster. This means that it is possible to call a function that will be invoke on another shard. We call such function a remote function.

Just like Gears functions, remote function must be declare on library load time using `redis.registerClusterFunction` API. The following example declares a remote function that returns the number of keys on the shard:

```js
redis.registerClusterFunction("dbsize", async(async_client) => {
    return client.block((async_client) => {
        return client.call("dbsize").toString();
    });
});
```

`redis.registerClusterFunction` gets the remote function name (that will be used later to call the remote function) and the remote function code. The remote function must be a Coroutine (async function) and it is executed on the background on the remote shard. For more information about async function, please refer to [Sync and Async Run](./Sync_Async.md) page.

We have couple of options to call a remote function, those options are expose through the async client which is given to a Coroutine:

* `async_client.runOnShards` - run the remote function on all the shards (including the current shard). Returns a promise that once resolve will give 2 nested arrays, the first contains another array with the results from all the shards and the other contains an array of errors (`[[res1, res2, ...],[err1, err2, ..]]`)
* `async_client.runOnKey` - run the remote function on the shard responsible for a given key. Returns a promise that once resolve will give the result from the remote function execution or raise an exception in case of an error.

The following example register a function that will return the total amount of keys on the cluster. The function will use the remote function define above:

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

First the function executes a remote function on all shards that returns the amount of keys on each shard, then the function summarize all the results and returns it.

The full code will look like this:

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

The function `test` will return the value of `x` regardless on which shard the function was executed on.

The remote function arguments and results are serialized in the following way:

1. If the argument is of `ArrayBuffer` type, the data will be sent as is.
2. Otherwise, RedisGears will try to serialize the give arguments (or the return value) as json using `JSON.stringify`. A serialization failure will cause an error to be raised.

## Execution Timeout

Remote functions has timeout to the results forever . The timeout can be configured using [remote-task-default-timeout](./../Configuration.md#remote-task-default-timeout). When using `async_client.runOnShards` API, the timeout will be added as error to the error array. When using `async_client.runOnKey`, a timeout will cause an exception to be raised.

## Remote Function Limitations

All the limitation listed on [Coroutine](./Sync_Async.md) are also applied to remote functions. Remote function come with some extra limitations:

* Remote function can only perform read operations. An attempt to perform a write operation will result in an error.
* Remote function are not promise to success (if the shard crashed for example). In such case a timeout error will be given.
