# RedisGears Function - Advance Topics

## Function Arguments

The arguments given on the [`TFCALL`](docs/commands.md#tfcall) command, after the function name, will be passed to the function callback. The following example shows how to implement a simple function that returns the value of a key whether its a string or a hash:

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

Example of running the following function:

```bash
127.0.0.1:6379> set x 1
OK
127.0.0.1:6379> TFCALL foo my_get 1 x
"1"
127.0.0.1:6379> hset h foo bar x y
(integer) 2
127.0.0.1:6379> TFCALL foo my_get 1 h
1) "foo"
2) "bar"
3) "x"
4) "y"

```

It is also possible to get all th arguments given to the function as a `JS` array. This is how we can extend the above example to except multiple keys and return their values:

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
127.0.0.1:6379> TFCALL foo my_get 2 x h
1) "1"
2) 1) "foo"
   2) "bar"
   3) "x"
   4) "y"
```

## Function Flags

It is possible to provide some information about the function behaviour on registration time. Such information is called function flags. The function flags is a optional argument that can be given after the function implementation. The supported flags are:
1. `redis.functionFlags.NO_WRITES` - indicating that the function performs not write commands. If this flag is on, it will be possible to run the function on read only replicas or on OOM. RedisGears force this flag behaviour, this means that any attempt to call a write command from within a function that has this flag will result in an exception.
2. `redis.functionFlags.ALLOW_OOM` - by default, RedisGears will not allow running any function on OOM. This flag allows overide this behaviour and running the function even on OOM. Enable this flag is considered unsafe and could cause Redis to bypass the maxmemory value. **User should only enable this flag if he knows for sure that his function do not consume memory** (for example, on OOM, it is safe to run a function that only deletes data).
3. `redis.functionFlags.RAW_ARGUMENTS` - by default, RedisGears will try to decode all function arguments as `JS` `String` and if it failed an error will be return to the client. When this flag is set, RedisGears will avoid String decoding and will pass the argument as `JS` `ArrayBuffer`.

The following example shows how to set the `redis.functionFlags.NO_WRITES` flag:

```js
#!js api_version=1.0 name=lib

redis.registerFunction('my_ping',
    function(client){
        return client.call('ping');
    },
    {
        flags: [redis.functionFlags.NO_WRITES]
    }
);
```

Run example:

```bash
127.0.0.1:6379> TFCALL foo my_ping 0
"PONG"
127.0.0.1:6379> config set maxmemory 1
OK
127.0.0.1:6379> TFCALL foo my_ping 0
"PONG"

```

## Library Configuration

When writing a library you might want to be able to provide a loading configuration, so that different users can use the same library with slightly different behaviour (without changing the base code). For example, assuming you writing a library that adds `__last_updated__` field to a hash (you can see how it can also be done with [databases triggers](databse_triggers.md)), the code will look like this:

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
127.0.0.1:6379> TFCALL lib hset k foo bar 0
(integer) 2
127.0.0.1:6379> hgetall k
1) "foo"
2) "bar"
3) "__last_update__"
4) "1658653125"
```

The problem with the above code is that the `__last_update__` field is hard coded, what if we want to allow the user to configure it at runtime? RedisGears allow specify library configuration at load time using `CONFIG` argument given to [`FUNCTION LOAD`](commands.md#tfunction-load) command. The configuration argument accept a string representation of a JSON object. The json will be provided to the library as a JS object under `redis.config` variable. We can change the above example to accept `__last_update__` field name as a library configuration. The code will look like this:

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

Notica that in the above example we first set `last_update_field_name` to `__last_update__`, this will be the default value in case not given by the configuration. Then we check if we have `last_update_field_name` in our configuration and if we do we use it. We can now upload our function with `CONFIG` argument:

```bash
> redis-cli -x TFUNCTION LOAD REPLACE CONFIG '{"last_update_field_name":"last_update"}' < <path to code file>
OK
```

And we can see that the last update field name is `last_update`:

```bash
127.0.0.1:6379> TFCALL lib hset h foo bar 0
(integer) 2
127.0.0.1:6379> hgetall h
1) "foo"
2) "bar"
3) "last_update"
4) "1658654047"
```

Notice, RedisGears only gives the library the json configuration, **its the library responsibility to verify the correctness of the given configuration**.

## Resp -> JS Conversion

When running Redis commands from within a RedisGears function using `client.call` API, the reply is parsed as resp3 reply and converted to JS object using the following rules:

| resp 3            | JS object type                                                                                                                                 |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `status`          | `StringObject` with a field called `__reply_type` and value `status` (or error if failed to convert to utf8)                                   |
| `bulk string`     | JS `String` (or error if failed to convert to utf8)                                                                                            |
| `Error`           | Raise JS exception                                                                                                                             |
| `long`            | JS big integer                                                                                                                                 |
| `double`          | JS number                                                                                                                                      |
| `array`           | JS array                                                                                                                                       |
| `map`             | JS object                                                                                                                                      |
| `set`             | JS set                                                                                                                                         |
| `bool`            | JS boolean                                                                                                                                     |
| `big number`      | `StringObject` with a field called `__reply_type` and value `big_number`                                                                       |
| `verbatim string` | `StringObject` with 2 additional fields: 1. `__reply_type` and value `verbatim` 2. `__format` with the value of the ext in the verbatim string (or error if failed to convert to utf8) |
| `null`            | JS null                                                                                                                                        |
|                   |                                                                                                                                                |

When running Redis commands from within a RedisGears function using `client.callRaw` API, the reply is parsed as resp3 reply and converted to JS object using the following rules:

| resp 3            | JS object type                                                                                                                                 |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `status`          | JS `ArrayBufffer` with a field called `__reply_type` and value `status`                                                                        |
| `bulk string`     | JS `ArrayBufffer`                                                                                                                              |
| `Error`           | Raise JS exception                                                                                                                             |
| `long`            | JS big integer                                                                                                                                 |
| `double`          | JS number                                                                                                                                      |
| `array`           | JS array                                                                                                                                       |
| `map`             | JS object                                                                                                                                      |
| `set`             | JS set                                                                                                                                         |
| `bool`            | JS boolean                                                                                                                                     |
| `big number`      | `StringObject` with a field called `__reply_type` and value `big_number`                                                                       |
| `verbatim string` | JS `ArrayBufffer` with 2 additional fields: 1. `__reply_type` and value `verbatim` 2. `__format` with the value of the ext in the verbatim string |
| `null`            | JS null                                                                                                                                        |
|                   |                                                                                                                                                |

## JS -> RESP Conversion

| JS type                                                          | RESP2         | RESP3                                  |
|------------------------------------------------------------------|---------------|----------------------------------------|
| `string`                                                         | `bulk string` | `bulk string`                          |
| `string` object with field `__reply_type=status`                 | `status`      | `status`                               |
| Exception                                                        | `error`       | `error`                                |
| `big integer`                                                    | `long`        | `long`                                 |
| `number`                                                         | `bulk string` | `double`                               |
| `array`                                                          | `array`       | `array`                                |
| `map`                                                            | `array`       | `map`                                  |
| `set`                                                            | `array`       | `set`                                  |
| `bool`                                                           | `long`        | `bool`                                 |
| `string` object with field`__reply_type=varbatim` and `__format=txt` | `bulk string` | `verbatim string` with format as `txt` |
| `null`                                                           | resp2 `null`  | resp3 `null`                           |

## Working with Binary Data

By default, RedisGears will decode all data as string and will raise error on failures. Though usefull for most users sometimes there is a need to work with binary data. In order to do so, the library developer has to considerations the following:

1. Binary function arguments
2. Binary command results
3. Binary keys names On [database triggers](databse_triggers.md)
4. Binary data on [stream consumers](stream_processing.md)

### Binary Function Arguments

It is possible to instruct RedisGears not to decode function arguments as `JS` `Strings` using [redis.functionFlags.RAW_ARGUMENTS](#function-flags) function flag. In this case, the function arguments will be given as `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
redis.registerFunction("my_set",
    (c, key, val) => {
        return c.call("set", key, val);
    },
    {
        flags: [redis.functionFlags.RAW_ARGUMENTS]
    }
);
```

The above example will allow us to set `key` and `val` even if those are binary data. Run example:

```bash
127.0.0.1:6379> TFCALL lib my_set 1 "\xaa" "\xaa"
"OK"
127.0.0.1:6379> get "\xaa"
"\xaa"
```

Notice that `call` function also except `JS` `ArrayBuffer` arguments.

### Binary Command Results

Getting function arguments as binary data is not enough. We might want to read binary data from Redis key. In order to do this we can use `callRaw` function that will not decode the result as `JS` `String` and instead will return the result as `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
redis.registerFunction("my_get", 
    (c, key) => {
        return c.callRaw("get", key);
    },
    {
        flags: [redis.functionFalgs.RAW_ARGUMENTS]
    }
);
```

The above example will be able to fetch binary data and return it to the user. Run example:

```bash
27.0.0.1:6379> set "\xaa" "\xaa"
OK
127.0.0.1:6379> TFCALL lib my_get 1 "\xaa"
"\xaa"
```

Notice that `JS` `ArrayBuffer` can be returned by RedisGears function, RedisGears will return it to the client as `bulk string`.

### Binary Keys Names On Database Triggers

On [database triggers](databse_triggers.md), if the key name that triggered the event is binary. The `data.key` field will be NULL. The `data.key_raw` field is always provided as `JS` `ArrayBuffer` and can be used in this case, example:

```js
#!js api_version=1.0 name=lib
/* The following is just an example, in general it is discourage to use globals. */
var n_notifications = 0;
var last_key = null;
var last_key_raw = null;
redis.registerKeySpaceTrigger("consumer", "", function(client, data) {
    if (data.event == "set") {
        n_notifications += 1;
        last_data = data.key;
        last_key_raw = data.key_raw;
    }
});

redis.registerFunction("notifications_stats", async function(){
    return [
        n_notifications,
        last_key,
        last_key_raw
    ];
});
```

Run example:

```bash
127.0.0.1:6379> set "\xaa" "\xaa"
OK
127.0.0.1:6379> TFCALL lib notifications_stats 0
1) (integer) 1
2) (nil)
3) "\xaa"
```

For more information, follow [database triggers](databse_triggers.md) page.

### Binary Data on Stream Consumers

On [stream consumers](stream_processing.md), if the key name is binary. The `data.stream_name` field will be NULL. The `data.stream_name_raw` field is always provided as `JS` `ArrayBuffer` and can be used in this case. In addition, if the content of the steam is binary, it will also appear as `null` under `data.record`. In this case it is possible to use `data.record` (which always exists) and contains the data as `JS` `ArrayBuffer`. Example:

```js
#!js api_version=1.0 name=lib
/* The following is just an example, in general it is discourage to use globals. */
var last_key = null;
var last_key_raw = null;
var last_data = null;
var last_data_raw = null;
redis.registerFunction("stats", function(){
    return [
        last_key,
        last_key_raw,
        last_data,
        last_data_raw
    ];
})
redis.registerStreamTrigger("consumer", new Uint8Array([255]).buffer, function(c, data){
    last_key = data.stream_name;
    last_key_raw = data.stream_name_raw;
    last_data = data.record;
    last_data_raw = data.record_raw;
})
```

Run Example:

```bash
127.0.0.1:6379> xadd "\xff\xff" * "\xaa" "\xaa"
"1659515146671-0"
127.0.0.1:6379> TFCALL foo stats 0
1) (nil)
2) "\xff\xff"
3) 1) 1) (nil)
      2) (nil)
4) 1) 1) "\xaa"
      2) "\xaa"

```
