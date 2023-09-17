---
title: "Quick start using RedisInsight"
linkTitle: "Quick start (RedisInsight)"
weight: 1
description: >
    Get started with triggers and functions using RedisInsight
---

Make sure that you have [Redis Stack installed](/docs/getting-started/install-stack/) and running. Alternatively, you can create a [free Redis Cloud account](https://redis.com/try-free/). The triggers and functions preview is available in the fixed subscription plan for the Google Cloud Asia Pacific (Tokyo) and AWS Asia Pacific (Singapore) regions.

## Connect to Redis Stack

Open the RedisInsight application, and connect to your database by clicking on its database alias.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-0.png">

## Load a library

Click on the triggers and functions icon and then on **+ Library** as shown below.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-1.png">

Add your code to the **Library Code** section of the right-hand panel and then click **Add Library**.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-2.png">

You'll see the following when the library was added:

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-3.png">

The `TFCALL` command is used to execute the JavaScript Function. If the command fails, an error will be returned. Click on the **>_ CLI**  button in the lower left-hand corner to open a console window and then run the command shown below.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-3a.png">

To update a library you can edit the library code directly in the interface by clicking on the edit (pencil) icon. When you save your changes, the libary will be reloaded.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-4.png">

## Uploading an external file

Click on the **+ Add Library** button as before and, instead of adding the code directly into the editor, click on the **Upload** button, select the file from your file browser, and then click on **Add Library**. The file needs to contain the header, which contains the engine identifier, the API version, and the library name: `#!js api_version=1.0 name=myFirstLibrary`.

<img src="/docs/interact/programmability/triggers-and-functions/images/tf-rdi-5.png">

## Creating triggers

Functions within Redis can respond to events using keyspace triggers. While the majority of these events are initiated by command invocations, they also include events that occur when a key expires or is removed from the database.

For the full list of supported events, please refer to the [Redis keyspace notifications page](https://redis.io/docs/manual/keyspace-notifications/#events-generated-by-different-commands/?utm_source=redis\&utm_medium=app\&utm_campaign=redisinsight_triggers_and_functions_guide).

The following code creates a new keyspace trigger that adds a new field to a new or updated hash with the latest update time.

```javascript
#!js name=myFirstLibrary api_version=1.0

function addLastUpdatedField(client, data) {
    if(data.event == 'hset') {
        var currentDateTime = Date.now();
        client.call('hset', data.key, 'last_updated', currentDateTime.toString());
    }
} 

// Register the KeySpaceTrigger 'AddLastUpdated' for keys with the prefix 'fellowship'
// with the callback function 'addLastUpdatedField'.
redis.registerKeySpaceTrigger('addLastUpdated', 'fellowship:', addLastUpdatedField);"
```

Update the existing library as before and then, using the RedisInsight console, add a new hash with the required prefix to trigger the function.

```Shell
> HSET fellowship:1 name "Frodo Baggins" title "The One Ring Bearer"
```

Run the `HGETALL` command to check if the last updated time is added to the example.

```Shell
> HGETALL fellowship:1
1) "name"
2) "Frodo Baggins"
3) "title"
4) "The One Ring Bearer"
5) "last_updated"
6) "1693238681822"
```
