- RedisGears Issue: [RedisGears/RedisGears#873](https://github.com/RedisGears/RedisGears/pull/896)

# RedisGears KSN (key space notification) API

This is an RFC to define the JS user facing API for KSN.

# Motivation

Redis has a module API that allow register for KSN events. All the supported events are listed [here](https://redis.io/docs/manual/keyspace-notifications/). The event provide the following information:

1. Event type (number represent the event type)
2. Event name (usually the command that called the event but could also be `expired`/`evicted`)
3. Key name

Inside the KSN callback, it is possible to perform any read operation but **it is forbidden to perform any write operations**. The reason for this is describe in details [here](https://github.com/redis/redis/pull/11199).

In order to still perform a write operations as a reaction to KSN. Redis provides a new API called post notification job. This new API allows (inside the notification callback) to register a job that will be invoked when the following conditions holds:

1. It is safe to perform any write operation.
2. The job will be called atomically along side the key space notification that triggered it.

This document describe options of exposing the described API to the JS user.

## Option 2 - KSN only

Probably not the option we will chose but just to mentioned it as an option. We can use only the KSN callback and disallow any write inside it (only reads). If the user wants to write he must do it on a background task which he will be trigger from within the KSN callback. The main disadvantage is that it is not possible to perform atomic write inside a KSN.

Example:

```js
redis.register_notifications_consumer("consumer", "", function(client, data){
    // This callback will be called directly on KSN.
    // It is impossible to perform writes on this callback
    // the callback can invoke a background task to write data but it will not be atomic.
    client.run_on_background(async function(async_client) {
        // here it is possible to perform writes (after entering an atomic block).
        async_client.block((client)=>{
                return client.call('set', 'x', '1');
        });
    });
});
```

## Option 2 - Post notification job only

In this option we will spare the JS user from all the details on KSN and post jobs. We will accumulate all the events for the user and call it on a post job where it is safe to write. The main disadvantage it that the JS user can not look at the data at the time of the notification. He can only read the data at the end, so in case of deleted key it will not be possible to see the deleted value. The advantage is API simplicity.

Example:

```js
redis.register_notifications_consumer("consumer", "", function(client, data){
    // this callback will be called after all the KSN finished.
    // it is safe to write and read.
    // the callback has access to the notification name and key name.
    // The callback has not access to the data at the time the notification happened.
    let key_name = data.key;
    client.call('set', 'x', '1'); // performing write command
});
```

## Option 3 - KSN callback + Post Notification job

This option is a combination on 2 and 1. The callback will be called on the KSN event without the ability to write. We will expose an API to register a post notification job in which is is safe to write.

Example:

```js
redis.register_notifications_consumer("consumer", "", function(client, data){
    // This callback will be called directly on KSN.
    // It is impossible to perform writes on this callback
    // the callback can register a post notification job
    // to write data atomically with the notification.
    client.register_post_notification_job(function(client) {
        // here we can write
        client.call('set', 'x', '1');
    });
});
```

## Option 4 - Simple API for regular users + advance API for advance users

This option is a combination on 2 and 4. We will implement a simple API for most users which will look like option 2. For advance users we will implement option 4.

Example:

```js
// Simple API for most users
redis.register_notifications_consumer("consumer", "", function(client, data){
    // this callback will be called after all the KSN finished.
    // it is safe to write and read.
    // the callback has access to the notification name and key name.
    // The callback has not access to the data at the time the notification happened.
    let key_name = data.key;
    client.call('set', 'x', '1'); // performing write command
});

// Advance API for advance users
redis.register_ksn_consumer("consumer", "", function(client, data){
    // This callback will be called directly on KSN.
    // It is impossible to perform writes on this callback
    // the callback can register a post notification job
    // to write data atomically with the notification.
    client.register_post_notification_job(function(client) {
        // here we can write
        client.call('set', 'x', '1');
    });
});
```
## Open Questions

- [ ] Should we try to catch infinite loops?
- [ ] Should we allow filtering by the event type on the rust level (before JS code is called)
