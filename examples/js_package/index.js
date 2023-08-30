#!js name=gears_example api_version=1.0

import { redis } from '@redis/gears-api';

var numberOfCalls = 0;

redis.registerFunction("foo", () => {
    return ++numberOfCalls;
});

redis.registerAsyncFunction(
  'asyncfoo', //Function name
  function(client, args) {
      console.log("Hello from async")
  }, //callback
  {
    description: 'description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } //optional arguments
);

redis.registerKeySpaceTrigger(
  'bar', // trigger name
  'keys:*', //key prefix
  function(client, data) {
    console.log("Got this key data updated: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data updated: " + data);
    }
  } //optional arguments
)

redis.registerStreamTrigger(
  'foobar', //trigger name
  'stream:*', //prefix
  function(client, data) {
    console.log("Got this stream data updated: " + data);
  },//callback
  {
    description: 'Description',
    window: 1,
    isStreamTrimmed: false
  } //optional arguments
)
