- RedisGears Issue: [RedisGears/RedisGears#937](https://github.com/RedisGears/RedisGears/pull/937)


# RedisGears Async Command Invocation


This is an RFC to summarise the options for exposing an API for async command invocation.


# Motivation


RedisGears allows the user to run a `JS` code on the server side. The `JS` is capable of interacting back with Redis in different ways. One of those ways are command invocation. Today it is already possible to invoke most of the Redis commands such as `get` and `set` but its not possible to invoke blocking commands like `blpop`. The RFC summarise the options by which we can expose blocking command invocation for RedisGears functions.


# Solutions


## Option 1


Use the existing API (`call` and `call_raw`) and in case the command gets blocked, return a promise object that will be fulfilled when we get the reply.


Example:


```JS
!JS name=lib api_version=1.0


redis.register_async_function("test", (client) => {
   return client.call("blpop", "l", "0");
});
```


The main disadvantage of this option is that we do not get a consistent return type, and the return type will be decided at runtime. Also there is no way for the user to specify that he does not want to block.


## Option 2


Add 2 new API's:


* `call_async` - runs a potentially blocking command and returns a promise object. If the command was not blocked, the promise object will be fulfilled with the command reply right away. If the command was blocked, the promise object will be fulfilled later on when we get the reply.
* `call_async_raw` - same as `call_async` but return raw results without utf8 decoding.


Example:


```JS
#!JS name=lib api_version=1.0


redis.register_async_function("test", (client) => {
   return client.call_async("blpop", "l", "0");
});
```


The main advantage over option 1 is that the return value is always a promise and does not depend on the command that was invoked or the database state. Also the user has a way to decide if he wants to allow blocking or not.


## Option 3


Same as option 2 but with flags option instead of adding more functions and making the provided API to verbose.


We will change the `call` function to also accept a dictionary that will contain the command and extra flags. The supported flags are:


1. `allow-async` - return a promise object as described in option 2.
2. `raw-results` - return the result without utf8 decoding.


We will still be able to use `call` with just a command as an argument and then the default values of the flags will be taken (no async and utf8 decoding enabled).


Example:


```JS
#!JS name=lib api_version=1.0


redis.register_async_function("test", (client) => {
   return client.call({
       command: ["blpop", "l", "0"],
       flags: [redis.callFlags.allowAsync]
   });
});
```


Notice that if we chose this option then we can drop `call_raw` and use instead:


```JS
client.call({
   command: ["get", "x"],
   flags: [redis.callFlags.rawResults]
})
```



