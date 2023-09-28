---
title: "Quick start using redis-cli"
linkTitle: "Quick start (redis-cli)"
weight: 2
description: >
    Get started with triggers and functions using redis-cli
aliases:
    - /docs/interact/programmability/triggers-and-functions/quick_start    
---

Make sure that you have [Redis Stack installed](/docs/getting-started/install-stack/) and running. Alternatively, you can create a [free Redis Cloud account](https://redis.com/try-free/?utm_source=redisio&utm_medium=referral&utm_campaign=2023-09-try_free&utm_content=cu-redis_cloud_users). The triggers and functions preview is available in the fixed subscription plan for the Google Cloud Asia Pacific (Tokyo) and AWS Asia Pacific (Singapore) regions.

## Connect to Redis Stack

```Shell
> redis-cli -h 127.0.0.1 -p 6379
```

## Load a library

Use the `TFUNCION LOAD` command to create a new library in your Redis instance.

```Shell
127.0.0.1:6379> TFUNCTION LOAD "#!js api_version=1.0 name=myFirstLibrary\n redis.registerFunction('hello', ()=>{ return 'Hello World'})"
OK
```

When the library is created successfully, an `OK` response is returned. Run the `TFUNCTION LIST` command to confirm your library was added to Redis.

```shell
> TFUNCTION LIST
1) 1) "api_version"
   2) "1.0"
   3) "cluster_functions"
   4) (empty list or set)
   5) "configuration"
   6) "null"
   7) "engine"
   8) "js"
   9) "functions"
   10) 1) "hello"
   11) "keyspace_triggers"
   12) (empty list or set)
   13) "name"
   14) "myFirstLibrary"
   15) "pending_async_calls"
   16) (empty list or set)
   17) "pending_jobs"
   18) "0"
   19) "stream_triggers"
   20) (empty list or set)
   21) "user"
   22) "default"
```

The `TFCALL` command is used to execute the JavaScript Function. If the command fails, an error will be returned.

```Shell
127.0.0.1:6379> TFCALL myFirstLibrary.hello 0
"Hello World"
```

To update the library run the `TFUNCTION LOAD` command with the additional parameter `REPLACE`.

```Shell
127.0.0.1:6379> TFUNCTION LOAD REPLACE "#!js api_version=1.0 name=myFirstLibrary\n redis.registerFunction('hello', ()=>{ return 'Hello World updated'})"
OK
```

## Uploading an external file

Use the `redis-cli` command to upload JavaScript from an external file. The file needs to contain the header, which contains the engine identifier, the API version, and the library name: `#!js api_version=1.0 name=myFirstLibrary`.

```JavaScript
#!js api_version=1.0 name=lib

redis.registerFunction('hello', ()=> {
  return 'Hello from an external file'
})
```

Use the `redis-cli -x` option to send the file with the command and use the `TFUNCTION LOAD REPLACE` to replace the inline library with the one from the `main.js` file.

```Shell
redis-cli -x TFUNCTION LOAD REPLACE < ./main.js
```

## Creating triggers

Functions within Redis can respond to events using keyspace triggers. While the majority of these events are initiated by command invocations, they also include events that occur when a key expires or is removed from the database.

For the full list of supported events, please refer to the [Redis keyspace notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands/?utm_source=redis\&utm_medium=app\&utm_campaign=redisinsight_triggers_and_functions_guide).

The following code creates a new keyspace trigger that adds a new field to a new or updated hash with the latest update time. 

Load the code in your database:

```redis Load keyspace example
TFUNCTION LOAD REPLACE "#!js name=myFirstLibrary api_version=1.0\n 
    function addLastUpdatedField(client, data) {
        if(data.event == 'hset') {
            var currentDateTime = Date.now();
            client.call('hset', data.key, 'last_updated', currentDateTime.toString());
        }
    } 
    redis.registerKeySpaceTrigger('addLastUpdated', 'fellowship:', addLastUpdatedField);" // Register the KeySpaceTrigger 'AddLastUpdated' for keys with the prefix 'fellowship' with a callback to the function 'addLastUpdatedField'
```

Add a new hash with the required prefix to trigger our function.

```Shell
127.0.0.1:6379> HSET fellowship:1 name "Frodo Baggins" title "The One Ring Bearer"
```

Check if the last updated time is added to the example.

```Shell
127.0.0.1:6379> HGETALL fellowship:1
1) "name"
2) "Frodo Baggins"
3) "title"
4) "The One Ring Bearer"
5) "last_updated"
6) "1693238681822"
```
