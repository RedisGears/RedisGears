# RedisGears-2.0

A [Redis module](https://redis.io/docs/modules/) that allows running a JS functions inside the Redis processes. The `JS` code is execute use [V8 `JS` engine](https://v8.dev/).

**Notice, RedisGears 2.0 is still under active development and not yet GA, the API might (and probably) change at the final GA version.**

## Run Using Docker

```bash
docker run -p 6379:6379 redislabs/redisgears:edge
```

## Build

### With Docker

Run the following on the main directory:
```bash
> docker build -t redisgears2 .
```

Then run the built image:
```bash
> docker run -p 6379:6379 redisgears2
```

### From Source

See the [build](docs/build_instructions.md) page for more information.

## Getting started

### Run JS code
The API expose by the module is very similar to the way [Redis Functions](https://redis.io/docs/manual/programmability/functions-intro/) is working. Lets write a simple `hello world` RedisGears function that return the string `hello world`:
```js
#!js name=lib api_version=1.0

redis.registerFunction('hello_world', function(){
    return 'hello_world';
});
```
The first line indicates the engine to use (`js`) and the library name (`lib`). The rest is the library code.


Assuming we put the following code on a file `lib.js`, we can register our function on RedisGears using `TFUNCTION LOAD` command:

```bash
> redis-cli -x TFUNCTION LOAD < ./lib.js
OK
```

And now we can execute our function using [`TFCALL`](docs/commands.md#rgfcal) command, the command gets the library name and the function name `.` separated:

```bash
> redis-cli TFCALL lib.hello_world 0
"hello_world"
```

Notice that [`TFCALL`](docs/commands.md#rgfcal) command arguments is very close to Redis [`FCALL`](https://redis.io/commands/fcall/) command, the only difference is that on RedisGears the command also gets the library name. The `0` represent the number of keys that will follow (which in our case is `0`).

### Calling Redis Commands Inside our Gears Function

It is possible to call Redis commands inside our gears function. The function gets as first argument a client object that allows interaction with Redis using `call` function. The following example executes a simple `PING` command and return the result:

```js
#!js name=lib api_version=1.0

redis.registerFunction('my_ping', function(client){
    return client.call('ping');
});
```

If we will try to send it to our running Redis instance, we will get the following error:
```bash
> redis-cli -x TFUNCTION LOAD < ./lib.js
(error) Library lib already exists
```

We get the error because the library with the same name already exists, we can use the `REPLACE` argument to replace the library with the new code:
```bash
> redis-cli -x TFUNCTION LOAD UPGRADE < ./lib.js
OK
```

And now we can invoke `my_ping` using [`TFCALL`](docs/commands.md#rgfcal) :
```bash
> redis-cli TFCALL lib.my_ping 0
"PONG"
```

### Whats next?

* [Create a development environment](docs/create_development_environment.md)
* [Commands](docs/commands.md)
* [Configuration](docs/configuration.md)
* [Advance Functions Topics](docs/function_advance_topics.md)
* [Sync and Async Run](docs/sync_and_async_run.md)
* [Stream Processing with RedisGears 2.0](docs/stream_processing.md)
* [Database triggers](docs/databse_triggers.md)
* [Cluster support](docs/cluster_support.md)
* [JS API](docs/js_api.md)
