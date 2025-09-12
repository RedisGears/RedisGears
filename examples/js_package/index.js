#!js name=gears_example api_version=1.0

import { redis, EventNotificationFlags } from '@redis/gears-api';

var numberOfCalls = 0;

redis.registerFunction("foo", () => {
    return ++numberOfCalls;
});

redis.registerAsyncFunction(
  'asyncfoo', //Function name
  async function(async_client, args) {
      console.log("Hello from async")
  }, //callback
  {
    description: 'description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } //optional arguments
);

redis.registerKeySpaceTrigger(
  'bar', // trigger name
  'keys*', //key prefix
  function(client, data) {
    console.log("Got this key data 1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data 2: " + data);
    },
  } //optional arguments
)

redis.registerKeySpaceTrigger(
  'onlynewnotifications', // trigger name
  '', //key prefix
  function(client, data) {
    console.log("Got this key data created1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data created2: " + data);
    },
    eventNotificationFlags: [EventNotificationFlags.NEW]
  } //optional arguments
)

redis.registerKeySpaceTrigger(
  'allnewnotifications', // trigger name
  '', //key prefix
  function(client, data) {
    console.log("Got this key data 1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data 2: " + data);
    },
    eventNotificationFlags: [EventNotificationFlags.NEW, EventNotificationFlags.ALL]
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
