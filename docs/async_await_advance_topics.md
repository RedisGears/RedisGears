# Async Await Advance Topics

RedisGears v1.2 introduces async-await support. Generally, RedisGears runs executions in background threads, the number of threads used for executions is configurable (using [ExecutionThreads](configuration.md#executionthreads) config value). When all the execution threads are busy, new execution will wait until an execution thread will take them and run them. So we do not want to get into a busy loop waiting for something to happen, such a loop will consume an execution thread and will not allow this thread to run new executions. This is what async-await is meant to solve. Async await allows us to wait for something to happen without consuming a thread from the executions thread pool.

Using async-await, it is possible to give a python coroutine as a function step. The coroutine execution goes to a dedicated thread that runs a dedicated event loop to schedule all coroutines. Once the coroutine finished, the execution goes back to normal processing. For a full introduction to async-await please refer to [Async Await Support](intro.md#Async-Await-Support). We are going to cover some advanced topics related to async-await and its interaction with RedisGears.

## Async Await on Sync executions

When register an execution, it is possible to mention running mode using [mode](functions.md#register) argument. The possible values are async, async_local, and sync. When execution mode is sync, it is still possible to use async-await. In such a case, the execution will start synchronously and will move to a background thread once it will need to run a coroutine. This feature allows us to chose, in runtime, whether or not we want to go to the background. Take, for example, the execution caching showed [here](intro#waiting-for-another-execution)
```python
{{ include('async_await/async_await-003.py')}}
```

This example is working perfectly, but it's inefficient, why? Because its mode is async, which means that even if there is no cache missed it will still go to a background thread. Fetching a key from the key space is considered a very fast operation and it's not worth going to the background for this. We can change the execution to be sync and only go to the background if needed (on cache missed). It will look like this:
```python
{{ include('async_await/async_await-004.py')}}
```

The change made the execution synchronous. It starts synchronously and runs the `CountStudents` function. The function first checks if we have the students count in the cache. If we do, the function will return the count. Otherwise, the function will return a coroutine that will count the number of students, cache it, and return it. So as long as the execution does not return the coroutine, it runs synchronously. This means that all cache hits will run synchronously and efficiently.
!!! important "Notice"
    Once sync execution moved the background it continues to run as async_local, which means that you can not assume the Redis lock is acquired or that everything is atomic. use [atomic](runtime.md#atomic) to be on the safe side.

## Sync with Multi Exec
The code above is very efficient, but will it work inside multi-exec or Lua? Lets try it:
!!! example "Example: Sync with Multi Exec"
	````
	127.0.0.1:6379> multi
	OK
	127.0.0.1:6379> RG.TRIGGER COUNT_STUDENTS
	QUEUED
	127.0.0.1:6379> exec
	1) (error) Error type: <class 'gears.error'>, Value: Creating async record is not allow
	````
We are getting an error. This is because we can not block the client inside a Multi Exec block (or inside Lua). This means that we can not go to the background (and use async-await). RedisGears protects you and will not allow you to do something that can cause Redis instability or crashes. In this case, gears prevent you from going to the background. This limitation exists also if we set the execution mode to async or (async_local). The difference is that if the mode is async (or async local), the execution would get the error even before starting. In the above example, the error happened after the execution started and only when it got to the point where it needs to go to the background. This means that you might not be able to complete the execution and it is dangerous because it might cause Redis to stay in an unstable state (with respect to the data). RedisGears provides you with the ability to check if you can use async-await. The function [isAsyncAllow](runtime.md#isAsyncAllow) will return true iff async-await can be used in the current execution. This will allow you to chose to act differently in case async-await is not possible (for example, you might choose to return immediately with an error if async-await is not possible).
!!! important "Notice"
    Today, [isAsyncAllow](runtime.md#isAsyncAllow) will return false only inside Multi Exec or Lua. Those might be changed in the future.