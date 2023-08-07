---
title: "Quick start"
linkTitle: "Quick start"
weight: 1
description: >
    Get started with triggers and functions
---

Use the `TFUNCION LOAD` command to create a new library in your Redis instance.

```Shell
127.0.0.1:6370> TFUNCTION LOAD "#!js api_version=1.0 name=myFirstLibrary\n redis.registerFunction('hello', ()=>{ return 'Hello World'})"
OK
```

When the library is created successfully, an `OK` response is returned.
The `TFCALL` command is used to execute the JavaScript Function. If the command fails, an error will be returned.

```Shell
127.0.0.1:6370> TFCALL myFirstLibrary.hello 0
"Hello World"
```

To update the library run the `TFUNCTION LOAD` command with the additional parameter `REPLACE`.

```Shell
127.0.0.1:6370> TFUNCTION LOAD REPLACE "#!js api_version=1.0 name=myFirstLibrary\n redis.registerFunction('hello', ()=>{ return 'Hello World updated'})"
OK
```

## Uploading an external file

Use the *redis-cli* to upload JavaScript in an external file. The file needs to contain the header containing the engine, the API version and the library name: `#!js api_version=1.0 name=myFirstLibrary`.

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

Functions within Redis can respond to events using key space triggers. While the majority of these events are initiated by command invocations, they also include events that occur when a key expires or is removed from the database.

For the full list of supported events, please refer to the [Redis Key Space notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands/?utm_source=redis\&utm_medium=app\&utm_campaign=redisinsight_triggers_and_functions_guide).

The following code creates a new key space trigger that adds a new field to a hash with the latest update time. 

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

```redis Set an example
HSET fellowship:1 
    name "Frodo Baggins" 
    title "The One Ring Bearer"
```

Check if the last updated time is added to the example.

```redis View result
HGETALL fellowship:1
```
