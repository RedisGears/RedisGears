# RedisGears-2.0

A [Redis module](https://redis.io/docs/modules/) that allows running a JS functions inside the Redis processes.

## Getting started
### Pre-requisite
1. [rust](https://www.rust-lang.org/tools/install)
2. [Redis v7.0.3 or above](https://redis.io/)

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

### Whats next?

* [Create a development environment](docs/create_development_environment.md)
* [Commands](docs/commands.md)
* [Configuration](docs/configuration.md)
* [Sync and Async Run](docs/sync_and_async_run.md)
* [Stream Processing with RedisGears 2.0](docs/stream_processing.md)