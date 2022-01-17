# Key Miss Event
 
One of the common use cases of cache is read through. The idea is that if the data is not in the cache, the cache will go and fetch it from another source. RedisGears 1.2 allows to achieve this caching strategy on Redis using `keymiss` event. In this tutorial we will show how to achieve a read through caching strategy using Redis and RedisGears. Our external data source will be simply another Redis server.
 
!!! important "Notice"
    The tutorial assumes you have a Redis server with RedisGears running on port 6379 and another Redis server running on port 6380.
 
## Register to `keymiss` Event
 
The first thing we need to do is to Register to `keymiss` event. This event will be triggered each time a user tried to fetch a none existing key. We can do it by specifying `keymiss` as an event to the `eventTypes` argument on the [`KeysReader`](readers.md#keysreader). The following will print a log message each time Redis tries to fetch an non existing key:
 
```python
{{ include('miss_event/miss_event-000.py')}}
```
 
!!! important "Notice"
    We could achieve the same effect using [command hook](commands_hook.md) but using `keymiss` event it is much more efficient. We only execute python code if the data does not exist on Redis so we will only reduce the performance on cache miss.
 
Notice that we use `mode=async_local`, when we get the `keymiss` event, we want to go into a background thread to fetch the data so we will not block the entire Redis server.
 
## Fetching Data on `keymiss` Event
 
Now we can fetch the data from another Redis server located on port 6380 and set it locally to the relevant key:
 
```python
{{ include('miss_event/miss_event-001.py')}}
```
 
!!! important "Notice"
    For simplicity we assume the data on the missed key is a simple string, we will show later how to make sure we get the `keymiss` event only for specific commands.
 
Running the example will look like this:
 
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
 
So it looks good, the client tries to fetch key `x`, we get key miss notification and go fetch it from another Redis. When the client tries to fetch it again the key is already there. But the experience is not smooth, the client needs to perform 2 commands in order to get the data, can we do any better?
 
## Commands Argument to `KeyReader`
 
In order to improve client experience RedisGears 1.2 introduce a new argument to [`KeysReader`](readers.md#keysreader) called `commands`. The new argument allows us to specify a list of commands on which we want to fire events, when the `commands` argument is used, RedisGears will only fire the events on those commands and will link the client executing the command with the executions that were triggered. This means that the client that executed the command will not get the reply until all the executions that were triggered as a result of the command were finished. In addition, RedisGears allows to veride the command reply using [`override_reply`](runtime.md#override_reply) function. Using those capabilities, we can change the example to give a much better user experience:
 
 
```python
{{ include('miss_event/miss_event-002.py')}}
```
 
Running the example will give the following user experience:
 
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
 
You can see that Redis has no keys in it, but executing `get x` command returns the result because Redis goes and fetches the data from another database.
 
!!! important "Notice"
    [`override_reply`](runtime.md#override_reply) can only be done once, so if it is invoked from multiple executions, only the first one will succeed and the others will failed with exception.
 
# Limitations
 
As with the [command hook](commands_hook.md) it is not possible to use `commands` argument on the following cases:
 
* If the **command has movable keys** and `prefix` argument was used
* If the **command is marked with noscript flag**
 
Also, the example above will not work inside Lua script or MULTI EXEC (because in those cases it's not possible to go to the background). In such case, RedisGears will not link the client with the executions and the client will simply get the reply:
 
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
 
Also notice that in such case, [`override_reply`](runtime.md#override_reply) will raise an exception. In our example you can see the exception on [`RG.DUMPREGISTRATION`](commands.md#rgdumpregistrations) output (`['Traceback (most recent call last):\\n', '  File \"<string>\", line 8, in fetch_data\\n', 'gears.error: Can not get command ctx\\n`):
 
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
