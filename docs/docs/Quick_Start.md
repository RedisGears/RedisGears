---
title: "Quick start"
linkTitle: "Quick start"
weight: 1
description: >
    Get started with Triggers and Functions
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

TODO