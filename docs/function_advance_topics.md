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

## Library Configuration

When writing a library you might want to be able to provide a loading configuration, so that different users can use the same library with slightly different behaviour (without changing the base code). For example, assuming you writing a library that adds `__last_updated__` field to a hash (you can see how it can also be done with [databases triggers](databse_triggers.md)), the code will look like this:

```js
#!js name=lib

redis.register_function("hset", function(client, key, field, val){
    // get the current time in ms
    var curr_time = client.call("time")[0];
    return client.call('hset', key, field, val, "__last_update__", curr_time);
});
```

Run example:

```bash
127.0.0.1:6379> RG.FUNCTION call lib hset k foo bar
(integer) 2
127.0.0.1:6379> hgetall k
1) "foo"
2) "bar"
3) "__last_update__"
4) "1658653125"
```

The problem with the above code is that the `__last_update__` field is hard coded, what if we want to allow the user to configure it at runtime? RedisGears allow specify library configuration at load time using `CONFIG` argument given to [`FUNCTION LOAD`](commands.md#rgfunction-load) command. The configuration argument accept a string representation of a JSON object. The json will be provided to the library as a JS object under `redis.config` variable. We can change the above example to accept `__last_update__` field name as a library configuration. The code will look like this:

```js
#!js name=lib

var last_update_field_name = "__last_update__"

if (redis.config.last_update_field_name !== undefined) {
    if (typeof redis.config.last_update_field_name != 'string') {
        throw "last_update_field_name must be a string";
    }
    last_update_field_name = redis.config.last_update_field_name
}

redis.register_function("hset", function(client, key, field, val){
    // get the current time in ms
    var curr_time = client.call("time")[0];
    return client.call('hset', key, field, val, last_update_field_name, curr_time);
});
```

Notica that in the above example we first set `last_update_field_name` to `__last_update__`, this will be the default value in case not given by the configuration. Then we check if we have `last_update_field_name` in our configuration and if we do we use it. We can now upload our function with `CONFIG` argument:

```bash
> redis-cli -x RG.FUNCTION LOAD UPGRADE CONFIG '{"last_update_field_name":"last_update"}' < <path to code file>
OK
```

And we can see that the last update field name is `last_update`: 

```bash
127.0.0.1:6379> RG.FUNCTION call lib hset h foo bar
(integer) 2
127.0.0.1:6379> hgetall h
1) "foo"
2) "bar"
3) "last_update"
4) "1658654047"
```

Notice, RedisGears only gives the library the json configuration, **its the library responsibility to verify the correcness of the given configuration**.