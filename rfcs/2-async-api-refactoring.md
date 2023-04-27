- RedisGears Issue: [RedisGears/RedisGears#873]()

# RedisGears JavaScript Async API refactoring

This is an RFC to summarise require changes to the async API.

# Motivation

Currently, when a user registers a RedisGears function, he can declare the function as a coroutine (async function). Once RedisGears see that the given function is a coroutine, if immediately blocks the client and gets the invocation to the background. When the function invocation finishes, the client gets unblocked and get the reply.

Another possibility that will cause the client to be blocked is if the function invocation returns a promise object. In this case the client will be unblocked when the promise will get fulfilled. Mostly, in such cases, the client gets block because it waits for some event to happened and not necessarily because of a background logic invocation.

When a client is blocked, **Redis will not read any more commands from this client till the client gets unblocked**.

Some of the clients libraries implements the concept of pipeline and shared connections. This means that requests from different clients might get pipeline and sent on the same connection. This causes problems if the client gets blocked. One single blocked client can cause all other clients to get stuck, waiting for the function invocation to finish.

# Solution

The first step of the solution is to disallow `RG.FCALL` to block. **`RG.FCALL` should never block because it might be sent on a shared connection and cause other command to be delayed**. In order to still keep the blocking capability, we will introduce a new command, `RG.FCALLASYNC`, clients need to be aware that this new command might block and must send it on a dedicated connection.

In addition, to be more explicit, we will not allow `redis.register_function` to register a coroutine. Only sync function will be allowed to be registered using `redis.register_function`. In order to register a coroutine, one will have to use `redis.register_async_function`. `redis.register_async_function` will work the same as `redis.register_function` with one exception, it will accept also a coroutine and invoke it on the background (while blocking the client).
