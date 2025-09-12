- RedisGears Issue: [RedisGears/RedisGears#1027](https://github.com/RedisGears/RedisGears/pull/1027)


# RedisGears Event Notification Flags API

This is an RFC to summarise the options for exposing an API for the
event notification flags.


# Motivation

Currently, the RedisGears module receives the event notifications, with
the command name used, key, and the event type. However, the command
name is ignored as of now and is never used; the event types the
module listener is subscribed to are hardcoded and there is no "NEW"
flag which might be useful. The "NEW" flag in particular is a flag that
currently is the most user-requested one among all. But the code
infrastructure doesn't allow to simply allow for a new flag to be
specified by the user: the JavaScript API has to be changed, there might
be implications to the backwards compatibility depending on the
implementation, and the implementation itself may vary based on the
requirements: for example, we might also stop ignoring the event
command name and also expose it for the user to subscribe.

# Suggested Solutions


## [Option 1](https://github.com/RedisGears/RedisGears/pull/1061)

Use the existing API of `redis.registerKeySpaceTrigger` and extend the
optional dictionary argument to also accept a list of event notification
flags, which the user wants to subscribe to. For this to happen, the
event notification flags must also be known by the user and easily
accessible in the JavaScript code, so an enum containing the event
notification flag values is exposed. The minor version of the JavaScript
API is bumped, what means full backwards compatibility: if the flags are
not specified by the user in the optional dictionary argument, the
behaviour of the older (current) JavaScript API remains, leaving to
exactly the same results as prior to the change.


Examples:


```JS
#!js name=lib api_version=1.2

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
    // The new optional argument to specify the event notification flags:
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
    // The new optional argument to specify the event notification flags:
    eventNotificationFlags: [EventNotificationFlags.NEW, EventNotificationFlags.ALL]
  } //optional
)
```

This has an advantage of solving the problem, but if we already go this
way to expose the flags, we might need to filter some of those values
in the original Redis enumeration variants, which we shouldn't expose
to the user.

Another drawback is that JavaScript doesn't have any means to check the
values for incompatibility with each other or any other sort of overlaps.
This will have to be done at deploy time, by RedisGears itself.

## Option 2 (Expose the event command)

However, if we already think about the first approach, we might as well
reconsider the event command argument of the notification, and expose it
too; just as well as we could allow the user's key space trigger to
subscribe only to the commands wanted when those were fired.

This also requires bumping the minor version of the JS API, and the
changes can be performed in a backwards-compatible way.

To implement this solution, we need to expose an optional argument which
may consist of the command name(s) as strings. Once there is an
event notification, the RedisGears will look for a key space trigger
registered with the event notification command names, filter the ones
which have subscribed to this particular command event, and only invoke
these.

Example:

```JS
#!js name=lib api_version=1.2

redis.registerKeySpaceTrigger(
  'onlyzsetnotifications', // trigger name
  '', //key prefix
  function(client, data) {
    console.log("Got this key data created1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data created2: " + data);
    },
    // The new optional argument to specify the event notification
    // command names:
    eventNotificationCommands: ["ZSET"]
  } //optional arguments
)
```

Maintaining a list of all possible Redis commands is unnecessary: even
though possible, it brings a burden to maintain while not bringing any
major user experience improvement.


## Option 3 (1 & 2 combined)

The third option is just to combine both the implementations into one.

We can change the `redis.registerKeySpaceTrigger` function to accept
both, the flags and the command names.

Example:


```JS
#!js name=lib api_version=1.2

redis.registerKeySpaceTrigger(
  'onlynewzsetnotifications', // trigger name
  '', //key prefix
  function(client, data) {
    console.log("Got this key data created1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data created2: " + data);
    },
    // The new optional argument to specify the event notification flags:
    eventNotificationFlags: [EventNotificationFlags.NEW],
    // The new optional argument to specify the event notification
    // command names:
    eventNotificationCommands: ["ZSET"]
  } //optional arguments
)
```

In this case, however, there is again, might be a possible overlap of
the values of both, that needs to be checked at deploy time.

## Option 4 (Listen to the "NEW" flag by default)

This is a **NOT** backwards-compatible way, which implies that all the
key space event triggers subscribed previously (so, existing in the user
code), after this change, will also be fired when there are conditions
for the "NEW" flag. For this we need to patch the current key-space
event callback in the module to also subscribe to the "NEW" events.
After that, all the user callbacks will fired more often, and if the old
behaviour is required, the user code will have to be adjusted to account
for the change in the behaviour and filter our the "NEW" events.

To allow for filtering out the "NEW" events (or any other event, for
that matter), we need to expose the event in the JS API, what also
requires a major version bump. The major version bump is required to
signalise to the user that the behaviour has changed and the user might
need to account for the changes manually.

As of now, the `data` object in the key space trigger callback is a
JavaScript object with members "event" (which is a string containing the
command name used to trigger the event), the key name (`key`) as a
string and a `key_raw` member which is also a key name but provided as
a byte array. This solution requires adding another member there, which
may be called, for example, `flags`, which would be a JavaScript array
of event notification flags, the same as from the previous solutions.

Having the additional `flags` member of the `data` object, would allow
the users to filter out the undesirable events, and so would allow to
account for the change in the default behaviour.

Example:


```JS
#!js name=lib api_version=2.0

redis.registerKeySpaceTrigger(
  'allnotificationsexceptnew', // trigger name
  '', //key prefix
  function(client, data) {
    if (data.flags.includes(EventNotificationFlags.NEW)) {
      return;
    }
    console.log("Got this key data created1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data created2: " + data);
    },
  } //optional arguments
)
```

## Summary

The first three implementations allow the user to control the key space
event trigger subscription much deeper than it is now. Both can be done
in a backwards-compatible way, both require additional checking and
review of the implementation.

The fourth suggested solution is the only one which suggests a breaking
change in the behaviour.
