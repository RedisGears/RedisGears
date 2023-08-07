---
title: "Library configuration"
linkTitle: "Library configuration"
weight: 6
description: >
    How to use configuration in JavaScript Functions
---

When writing a library, you may want to provide a loading configuration so that different users can use the same library with slightly different behaviour, without changing the base code. For example, assume you write a library that adds a `__last_updated__` field to a hash (you can see how it can also be done with [KeySpace triggers](/docs/interact/programmability/triggers-and-functions/concepts/triggers/keyspace_triggers/)), the code will look like this:

```js
#!js api_version=1.0 name=lib

redis.registerFunction("hset", function(client, key, field, val){
    // get the current time in ms
    var curr_time = client.call("time")[0];
    return client.call('hset', key, field, val, "__last_update__", curr_time);
});
```

Run example:

```bash
127.0.0.1:6379> TFCALL lib.hset k a b 0
(integer) 2
127.0.0.1:6379> hgetall k
1) "foo"
2) "bar"
3) "__last_update__"
4) "1658653125"
```

The problem with the above code is that the `__last_update__` field is hard coded. What if we want to allow the user to configure it at runtime? Triggers and functions provide for specifying a library configuration at load time using a `CONFIG` argument that is passed to the `TFUNCTION LOAD` command. The configuration argument accepts a string representation of a JSON object. The JSON will be provided to the library as a JS object under the `redis.config` variable. We can change the above example to accept the `__last_update__` field name as a library configuration. The code will look like this:

```js
#!js api_version=1.0 name=lib

var last_update_field_name = "__last_update__"

if (redis.config.last_update_field_name !== undefined) {
    if (typeof redis.config.last_update_field_name != 'string') {
        throw "last_update_field_name must be a string";
    }
    last_update_field_name = redis.config.last_update_field_name
}

redis.registerFunction("hset", function(client, key, field, val){
    // get the current time in ms
    var curr_time = client.call("time")[0];
    return client.call('hset', key, field, val, last_update_field_name, curr_time);
});
```

Notice that in the above example we first set `last_update_field_name` to `__last_update__`, the default value in cases where a value is not provided by the configuration. Then we check if we have `last_update_field_name` in our configuration and if we do we use it. We can now load our function with a `CONFIG` argument:

```bash
> redis-cli -x TFUNCTION LOAD REPLACE CONFIG '{"last_update_field_name":"last_update"}' < <path to code file>
OK
```

We can see that the last update field name is `last_update`:

```bash
127.0.0.1:6379> TFCALL lib.hset h a b 0
(integer) 2
127.0.0.1:6379> hgetall h
1) "a"
2) "b"
3) "last_update"
4) "1658654047"
```

Notice, triggers and functions only provides the library with the JSON configuration. **It's the library's responsibility to verify the correctness of the given configuration**.
