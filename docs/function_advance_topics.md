# RedisGears Function - Advance Topics

## Function Arguments

The arguments given on the [`RG.FUNCTION CALL`](docs/commands.md#rgfunction-call) command, after the function name, will be passed to the function callback. The following example shows how to implement a simple function that returns the value of a key whether its a string or a hash:

```js
#!js name=lib

redis.register_function('my_get', function(client, key_name){
    if (client.call('type', key_name) == 'string') {
        return client.call('get', key_name);
    }
    if (client.call('type', key_name) == 'hash') {
        return client.call('hgetall', key_name);
    }
    throw "Unsupported type";
});
```

Example of running the following function:

```bash
127.0.0.1:6379> set x 1
OK
127.0.0.1:6379> rg.function call foo my_get x
"1"
127.0.0.1:6379> hset h foo bar x y
(integer) 2
127.0.0.1:6379> rg.function call foo my_get h
1) "foo"
2) "bar"
3) "x"
4) "y"

```

It is also possible to get all th arguments given to the function as a `JS` array. This is how we can extend the above example to except multiple keys and return their values:

```js
#!js name=lib

redis.register_function('my_get', function(client, ...keys){
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
127.0.0.1:6379> rg.function call foo my_get x h
1) "1"
2) 1) "foo"
   2) "bar"
   3) "x"
   4) "y"
```

## Function Flags

It is possible to provide some information about the function behaviour on registration time. Such information is called function flags. The function flags is a optional argument that can be given after the function implementation. The supported flags are:
1. `no-writes` - indicating that the function performs not write commands. If this flag is on, it will be possible to run the function on read only replicas or on OOM. RedisGears force this flag behaviour, this means that any attempt to call a write command from within a function that has this flag will result in an exception.
2. `allow-oom` - by default, RedisGears will not allow running any function on OOM. This flag allows overide this behaviour and running the function even on OOM. Enable this flag is considered unsafe and could cause Redis to bypass the maxmemory value. **User should only enable this flag if he knows for sure that his function do not consume memory** (for example, on OOM, it is safe to run a function that only deletes data).

The following example shows how to set the `no-writes` flag:

```js
#!js name=lib

redis.register_function('my_ping', function(client){
    return client.call('ping');
},
["no-writes"]);
```

Run example:

```bash
127.0.0.1:6379> rg.function call foo my_ping
"PONG"
127.0.0.1:6379> config set maxmemory 1
OK
127.0.0.1:6379> rg.function call foo my_ping
"PONG"

```