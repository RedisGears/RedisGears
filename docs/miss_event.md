# Key Miss Event
 
A common use case for caching is read-through. If the data is not already in the cache, then the cache will fetch it from another source. RedisGears 1.2 uses the `keymiss` event to achieve this caching strategy with Redis.

This tutorial shows how to implement a read-through caching strategy using Redis and RedisGears. An additional Redis server will serve as the external data source.
 
!!! important "Notice"
    This tutorial assumes you have a Redis server with RedisGears running on port 6379 and another Redis server running on port 6380.
 
## Register to `keymiss` Event
 
First, register a function to the `keymiss` event. This event will trigger each time a user tries to fetch a nonexistent key. To do so, specify `keymiss` as an event in the `eventTypes` argument of the [`KeysReader`](readers.md#keysreader).

The following example logs a message each time Redis tries to fetch a nonexistent key:
 
```python
{{ include('miss_event/miss_event-000.py')}}
```
 
!!! important "Notice"
    You could achieve a similar effect with a [command hook](commands_hook.md), but the `keymiss` event method is much more efficient. It only executes Python code if the data does not already exist in Redis, so it only reduces performance on cache misses.
 
Notice that we use `mode=async_local`. When the `keymiss` event occurs, it will use a background thread to fetch the data. This avoids blocking the entire Redis server.
 
## Fetch Data on `keymiss` Event
 
Now fetch the data from another Redis server located on port 6380 and set it to the relevant key locally:
 
```python
{{ include('miss_event/miss_event-001.py')}}
```
 
!!! important "Notice"
    For simplicity, we assume the data on the missed key is a string. A later example will show how to get the `keymiss` event for specific commands.
 
This example outputs:
 
```
> redis-cli -p 6380
127.0.0.1:6380> set x 1
OK
127.0.0.1:6380> exit
 
> redis-cli -p 6379
127.0.0.1:6379> get x
(nil)
127.0.0.1:6379> get x
"1"
```
 
When the client tries to fetch the key `x`, it gets a key miss notification and then fetches it from another Redis server. When the client tries to fetch it again, the key is already there. However, the experience is not smooth since the client needs to perform 2 commands in order to get the data.
 
## Commands Argument to `KeyReader`
 
In order to improve the client experience, RedisGears 1.2 introduce a new argument to [`KeysReader`](readers.md#keysreader) called `commands`. It allows you to specify a list of commands on which to fire events. When the `commands` argument is used, RedisGears will only fire the events on those commands and will link the client executing the command with the executions that were triggered. The client that executed the command will not get the reply until all the executions that were triggered as a result of the command finish. In addition, RedisGears allows you to override the command reply using the [`override_reply`](runtime.md#override_reply) function.

With these capabilities, you can change the example to give a better user experience:
 
 
```python
{{ include('miss_event/miss_event-002.py')}}
```
 
Running the example now gives the following output:
 
```
> redis-cli -p 6380
127.0.0.1:6380> set x 1
OK
127.0.0.1:6380> exit
 
> redis-cli -p 6379
127.0.0.1:6379> keys *
(empty array)
127.0.0.1:6379> get x
"1"
```
 
Although this Redis server has no keys, executing the `get x` command returns the result because it fetches the data from another database.
 
!!! important "Notice"
    [`override_reply`](runtime.md#override_reply) can only be done once. If it is invoked from multiple executions, only the first one will succeed and the others will fail with an exception.
 
# Limitations
 
As with the [command hook](commands_hook.md), you cannot use the `commands` argument in the following cases:
 
* If the **command has movable keys** and the `prefix` argument was used
* If the **command is marked with the noscript flag**
 
The example above will also not work inside a Lua script or MULTI EXEC (because in those cases it's not possible to run in the background). In such cases, RedisGears will not link the client with the executions, and the client will simply get the reply:
 
```
127.0.0.1:6379> keys *
(empty array)
127.0.0.1:6379> multi
OK
127.0.0.1:6379(TX)> get x
QUEUED
127.0.0.1:6379(TX)> exec
1) (nil)
```
 
Also notice that in such cases, [`override_reply`](runtime.md#override_reply) will raise an exception. In this example, you can see the exception on [`RG.DUMPREGISTRATION`](commands.md#rgdumpregistrations) output (`['Traceback (most recent call last):\\n', '  File \"<string>\", line 8, in fetch_data\\n', 'gears.error: Can not get command ctx\\n`):
 
```
127.0.0.1:6379> RG.DUMPREGISTRATIONS
1)  1) "id"
   2) "0000000000000000000000000000000000000000-1"
   3) "reader"
   4) "KeysReader"
   5) "desc"
   6) (nil)
   7) "RegistrationData"
   8)  1) "mode"
       2) "async_local"
       3) "numTriggered"
       4) (integer) 1
       5) "numSuccess"
       6) (integer) 0
       7) "numFailures"
       8) (integer) 1
       9) "numAborted"
      10) (integer) 0
      11) "lastError"
      12) "['Traceback (most recent call last):\\n', '  File \"<string>\", line 8, in fetch_data\\n', 'gears.error: Can not get command ctx\\n']"
      13) "args"
      14) 1) "regex"
          2) "*"
          3) "eventTypes"
          4) 1) "keymiss"
          5) "keyTypes"
          6) (nil)
          7) "hookCommands"
          8) 1) "get"
   9) "PD"
  10) "{'sessionId':'0000000000000000000000000000000000000000-0', 'depsList':[]}"
  11) "ExecutionThreadPool"
  12) "DefaultPool"
```
