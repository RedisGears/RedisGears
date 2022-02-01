# Async Await Advanced Topics

RedisGears v1.2 introduces async-await support.

Generally, RedisGears runs executions in background threads. The number of execution threads is configurable with the [ExecutionThreads](configuration.md#executionthreads) config value. When all execution threads are busy, new executions will wait until a thread becomes available. However, you don't want to waste an execution thread on a loop of waiting. Async-await allows processes to wait for a triggering event without consuming a thread from the execution thread pool.

With async-await, a Python coroutine becomes a function step. The coroutine execution runs in a dedicated thread with an event loop scheduling all coroutines. Execution returns to normal processing when the coroutine finishes.

For a full introduction to async-await, refer to [Async Await Support](intro.md#async-await-support). The following sections will cover some advanced topics related to async-await and its interaction with RedisGears.

## Async Await on Sync executions

When you register an execution, you can set the execution mode with the [mode](functions.md#register) argument. The possible values are `async`, `async_local`, and `sync`.

Even when the execution mode is `sync`, you can still use async-await. The execution will start synchronously and will move to a background thread once it needs to run a coroutine. This feature allows you to choose, at runtime, whether or not to run in the background.

Take, for example, the execution caching shown [here](intro.html#waiting-for-another-execution):
```python
{{ include('async_await/async_await-003.py')}}
```

This example works, but it's inefficient. Because its mode is `async`, it will still go to a background thread even if there are no cache misses. Fetching a key from the key space is a fast operation, so it's not worth running in the background. If you change the execution mode to `sync`, processes will only run in the background if needed, such as on cache misses.

Here's the updated example:
```python
{{ include('async_await/async_await-004.py')}}
```

After these changes, the execution is synchronous. It starts synchronously and runs the `CountStudents` function. The function first checks if the students count is in the cache. If it is, the function will return the count. Otherwise, the function will return a coroutine that will count the number of students, cache it, and return it. So as long as the execution does not return the coroutine, it runs synchronously. This means that all cache hits will run synchronously and efficiently.
!!! important "Notice"
    Once a `sync` execution moves to the background, it continues to run as `async_local`. This means that you cannot assume the Redis lock is acquired or that everything is atomic. Use [atomic](runtime.md#atomic) to be on the safe side.

## Sync with Multi Exec
The code above is very efficient, but will it work inside multi-exec or Lua?

Follow this example to test it:
!!! example "Example: Sync with Multi Exec"
	````
	127.0.0.1:6379> multi
	OK
	127.0.0.1:6379> RG.TRIGGER COUNT_STUDENTS
	QUEUED
	127.0.0.1:6379> exec
	1) (error) Error type: <class 'gears.error'>, Value: Creating async record is not allow
	````
It returns an error because it cannot block the client inside a Multi Exec block (or inside Lua). This means that it cannot go to the background or use async-await. RedisGears will not allow you to do something that could cause Redis instability or crashes. In this case, RedisGears prevents the process from going to the background.

This limitation also exists for the other execution modes `async` and `async_local`. The difference is that if the mode is `async` (or `async_local`), the execution would get the error before starting.

In the above example, the error happened after the execution started and only when it reached the point where it needed to go to the background. In such cases, the execution might not complete. This is dangerous because it might cause Redis to stay in an unstable state (with respect to the data).

RedisGears provides the ability to check if you can use async-await. The function [isAsyncAllow](runtime.md#isAsyncAllow) returns true if async-await can be used in the current execution. This allows you to choose different actions if async-await is not possible. For example, you might want to return immediately with an error if async-await is not possible.
!!! important "Notice"
    Today, [isAsyncAllow](runtime.md#isAsyncAllow) will return false only inside Multi Exec or Lua. However, this may change in the future.
