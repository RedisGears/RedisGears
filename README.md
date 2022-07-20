# RedisGears-2.0

A [Redis module](https://redis.io/docs/modules/) that allows running a JS functions inside the Redis processes. The `JS` code is execute use [V8 `JS` engine](https://v8.dev/).

**Notice, RedisGears 2.0 is still under active development and not yet GA, the API might (and probably) change at the final GA version.**

## Build

### With Docker

Run the following on the main directoty:
```bash
> docker build -t redisgears2 .
```

Then run the built image:
```bash
> docker run -p 6379:6379 redisgears2
```

### From Source

### Pre-requisite
1. [rust](https://www.rust-lang.org/tools/install)
2. [Redis v7.0.3 or above](https://redis.io/)
3. libssl-dev
4. pkg-config
5. clang
6. wget

### Compile
Run the following on the main directoty:
```bash
> cargo build
```

### Run
Run the following on the main directoty:
```bash
> ./run.sh
```


## Getting started

### Run JS code
The API expose by the module is very similar to the way [Redis Functions](https://redis.io/docs/manual/programmability/functions-intro/) is working. Lets write a simple `hello world` RedisGears function that return the string `hello world`:
```js
#!js name=lib

redis.register_function('hello_world', function(){
    return 'hello_world';
});
```
The first line indicates the engine to use (`js`) and the library name (`lib`). The rest is the library code.


Assuming we put the following code on a file `lib.js`, we can register our function on RedisGears using `RG.FUNCTION LOAD` command:

```bash
> redis-cli -x RG.FUNCTION LOAD < ./lib.js
OK
```

And now we can execute our function using `RG.FUNCTION CALL` command, the command gets the library name and the function name:

```bash
> redis-cli RG.FUNCTION CALL lib hello_world
"hello_world"
```

### Calling Redis Commands Inside our Gears Function

It is possible to call Redis commands inside our gears function. The function gets as first argument a client object that allows interaction with Redis using `call` function. The following example executes a simple `PING` command and return the result:

```js
#!js name=lib

redis.register_function('my_ping', function(client){
    return client.call('ping');
});
```

If we will try to send it to our running Redis instance, we will get the following error:
```bash
> redis-cli -x RG.FUNCTION LOAD < ./lib.js
(error) Library lib already exists
```

We get the error because the library with the same name already exists, we can use the `UPGRADE` argument to upgrade the library with the new code:
```bash
> redis-cli -x RG.FUNCTION LOAD UPGRADE < ./lib.js
OK
```

And now we can invoke `my_ping` using `RG.FUNCTION CALL`:
```bash
> redis-cli RG.FUNCTION CALL lib my_ping
"PONG"
```

### Function Arguments

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

### Whats next?

* [Create a development environment](docs/create_development_environment.md)
* [Commands](docs/commands.md)
* [Configuration](docs/configuration.md)
* [Sync and Async Run](docs/sync_and_async_run.md)
* [Stream Processing with RedisGears 2.0](docs/stream_processing.md)