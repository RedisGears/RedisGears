# Commands Hook (experimental API)
 
!!! important "Notice"
   Command hook API is considered experimental, which means that it's not yet stable and might change in the future, use it with caution and at your own risk.
 
RedisGears v1.2 introduces the ability to hook vanila Redis commands with a custom implementation. This can be done using the new [`hook`]() parameter to the [CommandReader](readers.md#commandreader). The following allows to overide the `hset` command and add the current time as `_last_modified_` field:
```python
{{ include('command_hook/command_hook-000.py')}}
```
 
The example uses [`call_next`]() to call the original Redis `hset` command and it adds the `_last_modified_` as the last argument. Running the example will give the following
```
127.0.0.1:6379> hset k1 foo bar
"2"
127.0.0.1:6379> hgetall k1
1) "foo"
2) "bar"
3) "_last_modified_"
4) "1642332526"
```
 
We got the `_last_modified_` field inside the hash as expected.
!!! important "Notice"
   If you look carefully you will see that the API was changed compare to the original Redis `hset` command.
   First We get `"2"` reply instead of `"1"` indicating that 2 fields were added to the hash. Second We get the result as a string and not as a number. We leave it to the reader to fix and match the Redis original API.
 
## Hook Commands on Specific Key
Hook commands are not cheap, especially when we need to invoke python code. RedisGears gives you the ability to postpone calling python code by providing filters that allow you to filter out hooks on the C code itself (before calling the python code). One of those filters is key filtering. It is possible to specify a key prefix on which you want your hook to be triggered. The previous example can be apply on keys that starts with the prefix `person` by specify `keyprefix='person'` as an argument to the [`register`](functions.md#register):
```python
{{ include('command_hook/command_hook-002.py')}}
```
 
Registering the following code will result in all the hashes with key name starts with `person` to also have the `_last_modified_` field:
```
127.0.0.1:6379> hset person:1 foo bar
"2"
127.0.0.1:6379> hgetall person:1
1) "foo"
2) "bar"
3) "_last_modified_"
4) "1642340697"
127.0.0.1:6379> hset k1 foo bar
(integer) 1
127.0.0.1:6379> hgetall k1
1) "foo"
2) "bar"
 
```
 
!!! important "Notice"
   On commands that operate on multiple keys, it's enough that one of the keys matches the prefix to trigger the hook.
 
## Nested Hooks
 
It is possible to hook the same command multiple times. RedisGears will chain the hooks in such a way that the last hook that was registered will be triggered first. Each time we call [`call_next`]() the next hook in the chain will be invoked until we reach the original Redis command implementation. Let extend the example above to add another field that counts how many times the hash was modified:
```python
{{ include('command_hook/command_hook-000.py')}}
```
 
The example registers another execution that overrides the `hset` command and performs `hincrby` on the `_times_modified_` field. Then it calls [`call_next`]() to call, either the next hook, or the original command. Running the example (alongside the example above) will give the following:
```
127.0.0.1:6379> hset person:1 foo bar
"2"
127.0.0.1:6379> hgetall person:1
1) "_times_modified_"
2) "1"
3) "foo"
4) "bar"
5) "_last_modified_"
6) "1642341054"
127.0.0.1:6379> hset k1 foo bar
"1"
127.0.0.1:6379> hgetall k1
1) "_times_modified_"
2) "1"
3) "foo"
4) "bar"
```
 
You can see that on `person:1` both hooks were triggered while on `k1` only the second hook was triggered as expected.
 
## Hooks Limitations
On the following cases, it is not possible to hook a command:
 
* It is **not** possible to hook a command on the following cases:
   * If the **command has movable keys**
   * If the **command is marked with noscript flag**
* It is possible to hook a command with a none sync execution (see [execution mode](file:///home/meir/work/RedisGears/site/async_await_doc/functions.html#register)). but once its done, it is not possible to hook it again (perform a nested hook). Also, **hooking a command with none sync execution will cause it to not be invokable from within a Lua script on MULTI EXEC**.
 

