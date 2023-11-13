---
title: "Debugging"
linkTitle: "Debugging"
weight: 5
description: >
    Methods for debugging your Redis Stack functions
---
## Overview

There are two methods you can use to debug your Redis Stack functions:

1. Make judicious use of the `redis.log` function, which writes to the Redis log file.
1. Use Redis [pub/sub](/docs/interact/pubsub/).

### Use `redis.log`

If you have access to the Redis log files, `redis.log` is a good method to use when debugging. However, there is a drawback. Redis Cloud users do not have access to the Redis log files, and it's pretty common that only system administrators have access to them on self-hosted installations. Fortunately, you can also use Redis pub/sub, which will be discussed in the next section.

You don't have to do anything special to use `redis.log`, as it's always available. Here is an example:

```javascript
#!js api_version=1.0 name=lib

redis.registerFunction('hello', ()=> {
  redis.log('Hello log')
  return 'Hello from an external file'
})
```

After loading the library and executing the function with `TFCALL`, you'll see something like the following in your Redis log file:

```
45718:M 01 Nov 2023 07:02:40.593 * <redisgears_2> Hello log
```

### Use Redis pub/sub

If you don't have access to your Redis database log files, you can use pub/sub. The following example demonstrates how to use the [client.call](/docs/interact/programmability/triggers-and-functions/concepts/javascript_api/#clientcall) API to publish to a pub/sub channel.

```javascript
#!js api_version=1.0 name=lib

const logChannel = 'tfLogChannel'

function publog(client, message) {
  client.call('publish', logChannel, message)
}

redis.registerFunction('tflog', (client) => {
  publog(client, 'sample pub/sub log message')
  return 'sample'
})
```
In a CLI session, subscribe to the `tfLogChannel` channel and watch for messages.

```redis
$ redis-cli
127.0.0.1:6379> subscribe tfLogChannel
1) "subscribe"
2) "tfLogChannel"
3) (integer) 1
Reading messages... (press Ctrl-C to quit or any key to type command)
```

In another CLI session, load the library, replacing what was already there, and then call the new function:

```redis
127.0.0.1:6379> TFCALL lib.tflog 0
"sample"
```

You'll see the following in the previous CLI session:

```redis
1) "message"
2) "tfLogChannel"
3) "sample pub/sub log message"
```

There is a downside to using pub/sub. Redis pub/sub provides at-most-once message delivery semantics, which means that once a message is sent, it won't be sent again. So, if a message isn't consumed, it's gone forever. This is likely okay for debugging, but for the longer term, `redis.log` is the better solution for log persistence.