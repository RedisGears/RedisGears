---
title: "Triggers and Functions Examples"
linkTitle: "Examples"
weight: 5
description: >
    How Triggers and Functions can be used
---

Triggers and Functions enable the detection of changes to data as they happen and guarantee seamless execution of business logic at the data source. This ensures that new options to manipulate the data are possible and can be delivered for all the clients at the same time, while deployment and maintenance are simplified. Let's explore some industry-specific use cases where these capabilities shine:

- **Retail**: In the retail sector, a function can be developed to update the inventory immediately upon order receipt. This empowers businesses to accurately predict stock requirements for specific warehouses on any given day.
- **Travel**: For the travel industry, a trigger can be utilized to detect new flight bookings and efficiently load the relevant information into a queue for different consumers. Services can then leverage this data to provide recommendations for hotels, restaurants, car rental services, and more. Leveraging Redis geometries, powerful recommendation systems can offer localized and personalized suggestions.
- **Subscription Services**: In the realm of subscription services, employing a keyspace trigger can automatically identify users whose subscriptions have been renewed, seamlessly changing their status to active. Further operations can be performed on these users, such as adding them to a queue for the delivery of notifications or executing additional actions.

These examples highlight the practical application of Triggers and Functions in different industries, showcasing their value in streamlining processes and delivering efficient solutions. 

Using keyspace and Stream triggers to capture events and execute the desired JavaScript functions requires few lines of code to enrich applications with new behaviors. The following examples show how typical problems are solved with JavaScript.


## Auditing and logging

Redis Triggers can be used to detect changes to specific data structures, and log an auditing trail in the Redis log. The timestamp of the latest change can be updated in the same function.

```javascript
function alertUserChanged(client, data) {
    // detect the event and log an audit trail
    if (data.event == 'hset'){
        redis.log('User data changed: ' + data.key);
    }
    else if (data.event == 'del'){
        redis.log('User deleted: ' + data.key);
    }

    var curr_time = client.call("time")[0];

    // add the current timestamp to the Hash
    client.call('hset', data.key, 'last', curr_time);
}

redis.registerKeySpaceTrigger('alert_user_changed', 'user:', 
    alertUserChanged, {description: 'Report user data change.'});
```


## Enrich and transform data

Data can be extracted, enriched or transformed, and loaded again. As an example, upon insertion of a document in a Hash data structure, a Trigger can launch the execution of a Function that computes the number of words in the text (in the example, a simple tokenization is presented but the logic can be as complex as required). The counter is finally stored in the same Hash together with the original document. 

```javascript
function wordsCounter(client, data){
    text = client.call('hget', data.key, 'content');
    words = text.split(' ').length;
    client.call('hset', data.key, 'cnt', words.toString());
    redis.log('Number of words: ' + words.toString()); //This log is for demo purposes, be aware of spamming the log file in production
}

redis.registerKeySpaceTrigger('words_counter', 'doc:', 
    wordsCounter, {description: 'Count words in a document.'});
```


## Batch operations

JavaScript functions can be executed when required, for example as part of scheduled or periodic maintenance routines. An example could be deleting data identified by the desired pattern. 


```javascript
#!js api_version=1.0 name=utils


redis.registerAsyncFunction('del_keys', async function(async_client, pattern){
    var count = 0;
    var cursor = '0';
    do {
        async_client.block((client)=>{
            var res = client.call('scan', cursor, 'match', pattern);
            cursor = res[0];
            var keys = res[1];
            keys.forEach((key) => {
                client.call('del', key);
            });
        });
    } while(cursor != '0');
    return count;
});
```

The function `del_keys` performs an asynchronous batch scan of all the keys in the database matching the desired pattern, and enters a blocking section where it gets partial results. The asynchronous design of this function permits non-blocking execution, so other clients will not starve while the iteration is performed on a keyspace of arbitrary size. The function can be invoked as follows.

```text
TFCALLASYNC utils.del_keys 0 "user:*"
```

Actions of any kind can be performed on the keyspace or a subset of it.


## Stream Processing

The ability to detect and process new data pushed to Redis Streams enables real-time data manipulation and transformation. For example, the following example detects new events added to a Stream and increments a counter. This function is supported on clustered environments, note how the `{tickets}:ntickets` is co-located in the same shard as the Stream `tickets`.


```javascript
redis.registerStreamTrigger(
    "consumer", 
    "tickets", 
    function(client, data) {
        redis.log(JSON.stringify(data, (key, value) =>
            typeof value === 'bigint'
                ? value.toString()
                : value 
        ));
        client.call("INCR", "{tickets}:" + "ntickets");
    }, 
    {
        isStreamTrimmed: true,
        window: 3   
    }
);
```

Load the library including the previous function, and add some data to the Stream to increment a counter tracking the number of tickets.

```
XADD tickets * userid 181234 title Interstellar id 45256 price 7.99
```

It is also possible to read every new entry from a Redis Stream, transform and load it to a document. Indexed search and aggregation of Hashes and JSON data structures provide for multiple uses. The following example registers a Stream trigger that captures a website user activity in a Stream per user.  To test the behavior, create two users:

```
JSON.SET {user:181234}:events $ '{"actions":[]}'
JSON.SET {user:34524}:events $ '{"actions":[]}'
```

Import the JavaScript library:

```javascript
redis.registerStreamTrigger(
    "tracker", 
    "user:", 
    function(client, data) {
        try {
            var event = {};
            event["ts"] = data['id'][0].toString()
            event["action"] = data.record[0][1]

            client.call("JSON.ARRAPPEND", 
                        "{" + data['stream_name'] + "}:" + "events", 
                        "$.actions", 
                        JSON.stringify(event));
            return false;
        }
        catch(error){
            redis.log(error);
        }
    }, 
    {
        isStreamTrimmed: false,
        window: 3   
    }
);
```

And record some UI events, such as clicks on buttons:

```
XADD user:181234 * action "click:432"
XADD user:181234 * action "click:384"
XADD user:34524 * action "click:92"
XADD user:34524 * action "click:432"
```

The events will be recorded in JSON documents, co-located with the related Stream data source.

```
JSON.GET {user:34524}:events $.actions
"[[{\"ts\":\"1689076535787\",\"action\":\"click:92\"},{\"ts\":\"1689076540495\",\"action\":\"click:432\"}]]"
```

JSON documents can be indexed to enable cross-users searches and queries.


## Automatic Expire

Sometimes it is useful to enforce expiration time for data that by nature is short-lived. An example could be session data, or authentication tokens. A trigger fits the use case and can execute a function that sets the desired TTL.


```javascript
function automaticSessionExpiry(client, data){
    client.call('expire', data.key, '3600');
    redis.log('Session ' + data.key + ' has been automatically expired'); //This log is for demo purposes, be aware of spamming the log file in production
}

redis.registerKeySpaceTrigger('automaticSessionExpiry', 'session:', 
    automaticSessionExpiry, {description: 'Set the session TTL to one hour.'});
```
