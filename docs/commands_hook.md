# Commands Hook (experimental API)
 
!!! important "Notice"
    The command hook API is considered experimental, which means that it's not yet stable and might change in the future. Use it with caution and at your own risk.
 
RedisGears v1.2 introduces the ability to override vanilla Redis commands with custom implementations. Use the new [`hook`]() parameter with the [CommandReader](readers.md#commandreader) to do so.

The following code overrides the `hset` command and adds the current time as the `_last_modified_` field:
```python
{{ include('command_hook/command_hook-000.py')}}
```
 
This example uses [`call_next`]() to call the original Redis `hset` command and adds `_last_modified_` as the last argument. 

Running this example will give the following output:
```
127.0.0.1:6379> hset k1 foo bar
"2"
127.0.0.1:6379> hgetall k1
1) "foo"
2) "bar"
3) "_last_modified_"
4) "1642332526"
```
 
The `_last_modified_` field appears inside the hash, as expected.
!!! important "Notice"
    Notice the differences in output between this example and the original Redis `hset` command.
    First, the overridden `hset` command replies with `"2"` instead of `"1"`, indicating that 2 fields were added to the hash. Second, the result is a string instead of a number. The reader will need to fix the output to match the original Redis API.
 
## Hook Commands on Specific Key
Hook commands are not cheap, especially if you need to invoke Python code. RedisGears gives you the ability to postpone calling Python code by providing filters that allow you to filter out hooks on the C code itself (before calling the Python code). One of those filters is key filtering. You can specify a key prefix on which you want your hook to be triggered.

Specify `keyprefix='person'` as an argument to the [`register`](functions.md#register) function to apply the previous example to keys that start with the prefix `person`:
```python
{{ include('command_hook/command_hook-002.py')}}
```
 
If you register this code, all hashes with key names that start with `person` will also have the `_last_modified_` field:
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
    For commands that operate on multiple keys, only one of the keys needs to match the prefix to trigger the hook.
 
## Nested Hooks
 
It is possible to hook the same command multiple times. The hooks will run in the reverse order of their registration, so the hook that was registered last will trigger first. Each time you call [`call_next`](), the next hook in the chain will run until it reaches the original Redis command implementation.

Extend the example above to add another field that counts how many times the hash was modified:
```python
{{ include('command_hook/command_hook-001.py')}}
```
 
This example registers another execution that overrides the `hset` command and performs `hincrby` on the `_times_modified_` field. Then it calls [`call_next`]() to call either the next hook or the original command.

Run this example (alongside the example above) to get the following output:
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
 
Both hooks triggered for `person:1` while only the second hook triggered for `k1`, as expected.
 
## Hook Limitations 
* You **cannot** use command hooks in the following cases:
    * If the **command has movable keys** and `keyprefix` was used
    * If the **command is marked with the noscript flag**
* You can hook a command with a non-sync execution (see [execution mode](file:///home/meir/work/RedisGears/site/async_await_doc/functions.html#register)). However, once it's done, it is not possible to hook it again (perform a nested hook). Also, **hooking a command with non-sync execution will prevent its invocation from within a Lua script on MULTI EXEC**.
 

