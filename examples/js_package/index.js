#!js name=gears_example api_version=1.0

import { redis } from '@redis/gears-api';

var numberOfCalls = 0;

redis.registerFunction("foo", (client) => {
    client.call('incr', 'triggers_counter');
    return ++numberOfCalls;
});

redis.registerAsyncFunction(
  'asyncfoo', // Function name
  async function(async_client, args) {
      console.log("Hello from async");
      async_client.block((c) => c.call('incr', 'triggers_counter'));
      ++numberOfCalls;
  }, // callback
  {
    description: 'description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } // optional arguments
);

redis.registerKeySpaceTrigger(
  'bar', // trigger name
  'key', // key prefix
  function(client, data) {
    console.log("Got this key data updated1: " + data);
    client.call('incr', 'triggers_counter');
    ++numberOfCalls;
  }, // callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data updated2: " + data);
    client.call('incr', 'triggers_counter');
        ++numberOfCalls;
    }
  } // optional arguments
)

redis.registerStreamTrigger(
  'foobar', // trigger name
  'stream', // prefix
  function(client, data) {
    console.log("Got this stream data updated: " + data);
    client.call('incr', 'triggers_counter');
    ++numberOfCalls;
  },// callback
  {
    description: 'Description',
    window: 1,
    isStreamTrimmed: false
  } // optional arguments
)

