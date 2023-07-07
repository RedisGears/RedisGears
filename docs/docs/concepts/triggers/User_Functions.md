---
title: "User Functions"
linkTitle: "User Functions"
weight: 1
description: >
    Execute JavaScript functions via `TFCALL` or `TFCALLASYNC`
---


All [`TFCALL`](docs/commands.md#tfcall) command arguments that follow the function name are passed to the function callback. The following example shows how to implement a simple function that returns the value of a key of type string or hash:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('my_get', function(client, key_name){
    if (client.call('type', key_name) == 'string') {
        return client.call('get', key_name);
    }
    if (client.call('type', key_name) == 'hash') {
        return client.call('hgetall', key_name);
    }
    throw "Unsupported type";
});
```

Example of running the preceding function:

```bash
127.0.0.1:6379> set x 1
OK
127.0.0.1:6379> TFCALL lib.my_get 1 x
"1"
127.0.0.1:6379> hset h a b x y
(integer) 2
127.0.0.1:6379> TFCALL lib.my_get 1 h
1) "a"
2) "b"
3) "x"
4) "y"

```

It is also possible to get all the function arguments as a `JS` array. This is how we can extend the above example to accept multiple keys and return their values:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('my_get', function(client, ...keys){
    var results = [];
    keys.forEach((key_name)=> {
            if (client.call('type', key_name) == 'string') {
                results.push(client.call('get', key_name));
                return;
            }
            if (client.call('type', key_name) == 'hash') {
                results.push(client.call('hgetall', key_name));
                return;
            }
            results.push("Unsupported type");
        }
    );
    return results;

});
```

Run example:

```bash
127.0.0.1:6379> TFCALL foo.my_get 2 x h
1) "1"
2) 1) "a"
   2) "b"
   3) "x"
   4) "y"
```
