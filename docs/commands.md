# RedisGears Commands

RedisGears is operated by commands sent to the server with a [Redis client](glossary.md#client).

The following sections describe the supported commands.

| Command | Description |
| --- | --- |
| [`RG.ABORTEXECUTION`](#rgabortexecution) | Aborts execution |
| [`RG.CONFIGGET`](#rgconfigget) | Returns a configuration key |
| [`RG.CONFIGSET`](#rgconfigset) | Sets a configuration key |
| [`RG.DROPEXECUTION`](#rgdropexecution) | Removes execution |
| [`RG.DUMPEXECUTIONS`](#rgdumpexecutions) | Outputs executions |
| [`RG.DUMPREGISTRATIONS`](#rgdumpregistrations) | Outputs registrations |
| [`RG.GETEXECUTION`](#rggetexecution) | Returns the details of an execution |
| [`RG.GETRESULTS`](#rggetresults) | Returns the results from an execution |
| [`RG.GETRESULTSBLOCKING`](#rggetresultsblocking) | Blocks the client until execution ends |
| [`RG.INFOCLUSTER`](#rginfocluster) | Returns cluster information |
| [`RG.PYEXECUTE`](#rgpyexecute) | Executes Python functions and registers functions for event-driven processing |
| [`RG.PYSTATS`](#rgpystats) | Returns memory usage statistics |
| [`RG.PYDUMPSESSIONS`](#rgpydumpsessions) | Returns a summery of existing python sessions |
| [`RG.PYPROFILE STATS`](#rgpyprofilestats) | Returns a profiling statistics for a given session id |
| [`RG.PYPROFILE RESET`](#rgpyprofilereset) | Reset profiling statistics for a given session id |
| [`RG.REFRESHCLUSTER`](#rgrefreshcluster) | Refreshes a node's view of the cluster |
| [`RG.PYDUMPREQS`](#rgpydumpreqs) | Returns detailed information about requirements |
| [`RG.REFRESHCLUSTER`](#rgrefreshcluster) | Refreshes node's view of the cluster |
| [`RG.TRIGGER`](#rgtrigger) | Triggers execution of registration |
| [`RG.TRIGGERONKEY`](#rgtriggeronkey) | Triggers execution of registration on a given key |
| [`RG.UNREGISTER`](#rgunregister) | Removes registration |
| [`RG.CLEARREGISTRATIONSSTATS`](#rgclearregistrationsstats) | Clears stats from all registrations |
| [`RG.PAUSEREGISTRATIONS`](#rgpauseregistrations) | Pause a registrations by ids |
| [`RG.UNPAUSEREGISTRATIONS`](#rgunpauseregistrations) | Unpause a registrations by ids |

**Syntax Conventions**

We use the following conventions to describe the RedisGears Redis API:

* `COMMAND`: a command
* `<mandatory>`: a mandatory argument
* `[optional]`: an optional argument
* `"`: a literal double quote character
* `|`: an exclusive or
* `...`:  more of the same as before

## RG.ABORTEXECUTION
The **RG.ABORTEXECUTION** command aborts the [execution](functions.md#execution) of a function in mid-flight.

**Redis API**

```
RG.ABORTEXECUTION <id>
```

_Arguments_

* _id_: the [execution ID](functions.md#execution-id) to abort

_Return_

A simple 'OK' if successful, or an error if the execution does not exist or had already finished.

**Examples**

```
redis> RG.ABORTEXECUTION 0000000000000000000000000000000000000000-1
OK
```

## RG.CONFIGGET
The **`RG.CONFIGGET`** command returns the value of one or more built-in [configuration](configuration.md) or a user-defined options.

**Redis API**

```
RG.CONFIGGET <key> [...]
```

_Arguments_

* _key_: the configuration option to fetch

_Return_

An array with an entry per key. The entry is a string being the value or an error for undefined options.

**Examples**

```
redis> RG.CONFIGGET ProfileExecutions
1) (integer) 0
redis> RG.CONFIGGET foo
1) (error) (error) Unsupported config parameter: foo
```

## RG.CONFIGSET
The **`RG.CONFIGSET`** command sets the value of one ore more built-in [configuration](configuration.md) or user-defined options.

**Redis API**

```
RG.CONFIGSET <key> <value> [...]
```

_Arguments_

* _key_: the configuration option to set
* _value_: the new value

_Return_

An array with an entry per key. The entry is simple 'OK' string, or an error if the option can't be configured in runtime.

**Examples**

```
redis> RG.CONFIGSET ProfileExecutions 1
1) OK
redis> RG.CONFIGSET foo bar
1) OK - value was saved in extra config dictionary
redis> RG.CONFIGGET foo
1) "bar"
```

## RG.DROPEXECUTION
The **RG.DROPEXECUTION** command removes the [execution](functions.md#execution) of a function from the executions list.

**Redis API**

```
RG.DROPEXECUTION <id>
```

_Arguments_

* _id_: the [execution ID](functions.md#execution-id) to remove

_Return_

A simple 'OK' if successful, or an error if the execution does not exist or is still running.

**Examples**

```
redis> RG.DROPEXECUTION 0000000000000000000000000000000000000000-1
OK
```

## RG.DUMPEXECUTIONS
The **RG.DUMPEXECUTIONS** command outputs the list of [function executions](functions.md#execution). The executions list's length is capped by the [MaxExecutions](configuration.md#maxexecutions) configuration option.

**Redis API**

```
RG.DUMPEXECUTIONS
```

_Return_

An array containing an entry per execution. Entry consist of alternating key names and value entries as follows:

* **executionId**: the [execution ID](functions.md#execution-id)
* **status**: the [status](functions.md#execution-status)
* **registered**: indicates whether this is a [registered execution](functions.md#registration)

**Examples**

```
redis> RG.DUMPEXECUTIONS
1) 1) "executionId"
   2) "0000000000000000000000000000000000000000-0"
   3) "status"
   4) "done"
   5) "registered execution"
   6) (integer) 1
2) 1) "executionId"
   2) "0000000000000000000000000000000000000000-1"
   3) "status"
   4) "running"
   5) "registered execution"
   6) (integer) 1
```

## RG.DUMPREGISTRATIONS
The **RG.DUMPREGISTRATIONS** command outputs the list of [function registrations](functions.md#registration).

**Redis API**

```
RG.DUMPREGISTRATIONS
```

_Return_

An array with an entry per registration. Each entry is made of alternating key name and value entries as follows:

* **id**: the [registration ID](functions.md#registration-id)
* **reader**: the [reader](readers.md)
* **desc**: the description
* **RegistrationData**: an array of the following:
    * **mode**: registration mode
    * **numTriggered**: a counter of triggered executions
    * **numSuccess**: a counter of successful executions
    * **numFailures**: a counter of failed executions
    * **numAborted**: a counter of aborted executions
    * **lastRunDurationMS**: duration in milliseconds of the last execution
    * **totalRunDurationMS**: total run time in milliseconds
    * **avgRunDurationMS**: average execution runtime in milliseconds
    * **lastEstimatedLagMS**: Only on Streams, give the last batch lag (the time difference from the moment the first batch entry enters the stream to the time the batch was finished processing)
    * **avgEstimatedLagMS**: Only on Streams, average lag.
    * **lastError**: the last error returned
    * **args**: reader-specific arguments
* **PD**: private data

**Examples**

```
redis> RG.DUMPREGISTRATIONS
1)  1) "id"
    2) "0000000000000000000000000000000000000000-2"
    3) "reader"
    4) "KeysReader"
    5) "desc"
    6) (nil)
    7) "RegistrationData"
    8)  1) "mode"
        2) "async"
        3) "numTriggered"
        4) (integer) 0
        5) "numSuccess"
        6) (integer) 0
        7) "numFailures"
        8) (integer) 0
        9) "numAborted"
       10) (integer) 0
       11) "lastError"
       12) (nil)
       13) "args"
       14) 1) "regex"
           2) "*"
           3) "eventTypes"
           4) (nil)
           5) "keyTypes"
           6) (nil)
    9) "PD"
   10) "{'sessionId':'0000000000000000000000000000000000000000-3', 'depsList':[]}"
```

## RG.GETEXECUTION
The **RG.GETEXECUTION** command returns the execution [execution](functions.md#execution) details of a function that's in the executions list.

**Redis API**

```
RG.GETEXECUTION <id> [SHARD|CLUSTER]
```

_Arguments_

* _id_: the [execution ID](functions.md#execution-id) to get
* _SHARD_: only gets the local execution (default in _stand-alone_ mode)
* _CLUSTER_: collects all executions from shards (default in _cluster_ mode)

!!! tip "RedisGears Trivia"
    When called with the `CLUSTER` argument, RedisGears runs a RedisGears function that collects the shards' execution plans... self-reference is so paradoxical :)

_Return_

An array if successful, or an error if the execution does not exist or is still running. The array consists of alternating key name and value entries as follows:

* **shard_id**: the shard's identifier in the cluster
* **execution_plan**: the plan of execution is made of:
    * **status**: the [status](functions.md#execution-status)
    * **shards_received**: number of shards that received the execution
    * **shards_completed**: number of shards that completed the execution
    * **results**: count of results returned
    * **errors**: count of errors raised
    * **total_duration**: total execution duration in milliseconds
    * **read_duration**: reader execution duration in milliseconds
    * **steps**: the plan's steps are made of the following:
        * **type**: step type
        * **duration**: the step's duration in milliseconds (0 when [ProfileExecutions](configuration.md#profileexecutions) is disabled)
        * **name**: step callback
        * **arg**: step argument

**Examples**

```
redis> RG.PYEXECUTE "GB().run()" UNBLOCKING
"0000000000000000000000000000000000000000-4"
redis> RG.GETEXECUTION 0000000000000000000000000000000000000000-4
1) 1) "shard_id"
   2) "0000000000000000000000000000000000000000"
   3) "execution_plan"
   4)  1) "status"
       2) "done"
       3) "shards_received"
       4) (integer) 0
       5) "shards_completed"
       6) (integer) 0
       7) "results"
       8) (integer) 0
       9) "errors"
      10) (empty list or set)
      11) "total_duration"
      12) (integer) 0
      13) "read_duration"
      14) (integer) 0
      15) "steps"
      16) 1) 1) "type"
             2) "collect"
             3) "duration"
             4) (integer) 0
             5) "name"
             6) "collect"
             7) "arg"
             8) ""
          2) 1) "type"
             2) "map"
             3) "duration"
             4) (integer) 0
             5) "name"
             6) "RedisGearsPy_PyCallbackMapper"
             7) "arg"
             8) "<function GearsBuilder.run.<locals>.<lambda> at 0x7f8b8f869880>"
          3) 1) "type"
             2) "map"
             3) "duration"
             4) (integer) 0
             5) "name"
             6) "RedisGearsPy_ToPyRecordMapper"
             7) "arg"
             8) ""
```

## RG.GETRESULTS
The **RG.GETRESULTS** command returns the [results](functions.md#results) and errors from of the execution [execution](functions.md#execution) details of a function that's in the executions list.

**Redis API**

```
RG.GETRESULTS <id>
```

_Arguments_

* _id_: the [execution ID](functions.md#execution-id) to get

_Return_

An array if successful, or an error if the execution does not exist or is still running. The reply array is made of two sub-arrays: one for results and the other for errors.

**Examples**

```
redis> RG.GETRESULTS 0000000000000000000000000000000000000000-4
1) (empty list or set)
2) (empty list or set)
```

## RG.GETRESULTSBLOCKING
The **RG.GETRESULTSBLOCKING** command cancels the `UNBLOCKING` argument of the [`RG.PYEXECUTE`](#rgpyexecute) command. The calling client is blocked until execution ends and is sent with any results and errors then.

**Redis API**

```
RG.GETRESULTSBLOCKING <id>
```

_Arguments_

* _id_: the [execution ID](functions.md#execution-id) to block upon

_Return_

An array of if successful, or an error if the execution does not exist. The reply array is made of two sub-arrays: one for results and the other for errors.

**Examples**

```
redis> RG.GETRESULTS 0000000000000000000000000000000000000000-4
1) (empty list or set)
2) (empty list or set)
```

## RG.INFOCLUSTER
The **RG.INFOCLUSTER** command outputs information about the cluster.

**Redis API**

```
RG.INFOCLUSTER
```

_Return_

An array that consists of alternating key name and value entries as follows:

* **MyId**: the shard's identifier in the cluster
* An array with an entry for each of the cluster's shards as follows:
    * **id**: the shard's identifier in the cluster
    * **ip**: the shard's IP address
    * **port**: the shard's port
    * **unixSocket**: the shard's UDS
    * **runid**: the engine's run identifier
    * **minHslot**: lowest hash slot served by the shard
    * **maxHslot**: highest hash slot served by the shard

**Examples**

```
127.0.0.1:30001> RG.INFOCLUSTER
1) "MyId"
2) "15f41d945e9e76175dd92bbfde27ded6bcfe53df"
3) 1)  1) "id"
       2) "15f41d945e9e76175dd92bbfde27ded6bcfe53df"
       3) "ip"
       4) "127.0.0.1"
       5) "port"
       6) (integer) 30001
       7) "unixSocket"
       8) "None"
       9) "runid"
      10) "a07592a2e979d8c7d028061fd7c6468463aaf79e"
      11) "minHslot"
      12) (integer) 0
      13) "maxHslot"
      14) (integer) 5460
   2)  1) "id"
       2) "b57e92c693e285b91b3f9cbccb9e8ae71b54516d"
       3) "ip"
       4) "127.0.0.1"
       5) "port"
       6) (integer) 30003
       7) "unixSocket"
       8) "None"
       9) "runid"
      10) "0dedafb5825f51eb1c692b4046abaa1c3c529037"
      11) "minHslot"
      12) (integer) 10923
      13) "maxHslot"
      14) (integer) 16383
   3)  1) "id"
       2) "0525c8651300850b73789f721912bf1eb236b51e"
       3) "ip"
       4) "127.0.0.1"
       5) "port"
       6) (integer) 30002
       7) "unixSocket"
       8) "None"
       9) "runid"
      10) "4a2bc1c3d269dbb1f007ee85da53d60e77b9ac35"
      11) "minHslot"
      12) (integer) 5461
      13) "maxHslot"
      14) (integer) 10922
```

## RG.PYEXECUTE
The **RG.PYEXECUTE** command executes a Python [function](functions.md#function).

**Redis API**

```
RG.PYEXECUTE "<function>" [UNBLOCKING] [ID <id>] [DESCRIPTION <description>] [UPGRADE] [REPLACE_WITH id] [REQUIREMENTS "<dep> ..."]
```

_Arguments_

* _function_: the Python function
* _UNBLOCKING_: doesn't block the client during execution
* _ID_: [Session](glossary.html#session) unique ID (if not given, RedisGears will generate one)
* _DESCRIPTION_: Optional [Session](glossary.html#session) description
* _UPGRADE_: If the session with this name already exists, replace it.
* _REPLACE_WITH_: Set the new [Session](glossary.html#session) as a replacement of the session give by this argument.
* _FORCE_REINSTALL_REQUIREMENTS_: Force re-installation of all the requirements. When this option is used, RedisGears will reset the python interpreter module cache after re-installation is finished. The python interpreter then reloads the modules. An important limitation of this option is that **It is not possible to have multiple versions of the same requirement at the same time**. If one registration uses an old version of a requirement and another registration upgrades it, then after a restart (or before, on some rare cases) both will have the new version. If the new version is **not** backwards compatible **the code may fail due to errors**. This option is available as of v1.2.4.
* _REQUIREMENTS_: this argument ensures that list of dependencies it is given as an argument is installed on each shard before execution

_Return_

An error is returned if the function can't be parsed, in addition to those generated by non-RedisGears functions used.

When used in `UNBLOCKING` mode reply is an [execution ID](functions.md#execution-id).

Any results and errors generated by the function are returned as an array made of two sub-arrays: one for results and the other for errors.

A simple 'OK' string is returned if the function has no output (i.e. it doesn't consist of any functions with the [run](functions.md#run) action).

**Examples**

```
redis> RG.PYEXECUTE "GB().run()"
1) (empty list or set)
2) (empty list or set)
```

## RG.PYSTATS
The **RG.PYSTATS** command returns memory usage statistics from the [Python interpreter](runtime.md#python-interpreter).

**Redis API**

```
RG.PYSTATS
```

_Return_

An array that consists of alternating key name and value entries as follows:

* **TotalAllocated**: a total of all allocations over time in bytes
* **PeakAllocated**: the peak value of allocations
* **CurrAllocated**: the currently allocated memory in bytes

**Examples**

```
redis> RG.PYSTATS
1) "TotalAllocated"
2) (integer) 113803317
3) "PeakAllocated"
4) (integer) 8432603
5) "CurrAllocated"
6) (integer) 5745816
```

## RG.PYDUMPSESSIONS
The **RG.PYDUMPSESSIONS** command returns a summary of existing python [sessions](glossary.html#session). A python session is created whenever the [RG.PYEXECUTE](#rgpyexecute) command is invoked, and then shared with all registrations/executions created by this command.

**Redis API**

```
RG.PYDUMPSESSIONS [TS] [VERBOSE] [SESSIONS s1 s2 ...]
```

_Arguments_

* _TS_: see session which was deleted but not yet freed (because there is still executions which created by the session and was not yet finished).
* _VERBOSE_: see a full information about requirements and registrations.
* _SESSIONS_: must be given last. When specified, return only sessions that appears in the list.

_Return_

An array that consists of alternating key name and value entries representing information about the session.

**Examples**

```
127.0.0.1:6379> RG.PYDUMPSESSIONS
1)  1) "ID"
    1) "test"
    2) "sessionDescription"
    3) (nil)
    4) "refCount"
    5) (integer) 1
    6) "Linked"
    7) "true"
    8) "TS"
   1)  "false"
   2)  "requirementInstallationNeeded"
   3)  (integer) 0
   4)  "requirements"
   5)  (empty array)
   6)  "registrations"
   7)  1) "0000000000000000000000000000000000000000-3"
127.0.0.1:6379> RG.PYDUMPSESSIONS VERBOSE SESSIONS test
1)  1) "ID"
    2) "test"
    3) "sessionDescription"
    4) (nil)
    5) "refCount"
    6) (integer) 1
    7) "Linked"
    8) "true"
    9) "TS"
   10) "false"
   11) "requirementInstallationNeeded"
   12) (integer) 0
   13) "requirements"
   14) (empty array)
   15) "registrations"
   16) 1)  1) "id"
           2) "0000000000000000000000000000000000000000-3"
           3) "reader"
           4) "CommandReader"
           5) "desc"
           6) (nil)
           7) "RegistrationData"
           8)  1) "mode"
               2) "async"
               3) "numTriggered"
               4) (integer) 1
               5) "numSuccess"
               6) (integer) 1
               7) "numFailures"
               8) (integer) 0
               9) "numAborted"
              10) (integer) 0
              11) "lastRunDurationMS"
              12) (integer) 0
              13) "totalRunDurationMS"
              14) (integer) 0
              15) "avgRunDurationMS"
              16) "0"
              17) "lastError"
              18) (nil)
              19) "args"
              20) 1) "trigger"
                  2) "test"
                  3) "inorder"
                  4) (integer) 0
           9) "ExecutionThreadPool"
          10) "DefaultPool"
```

## RG.PYPROFILE STATS
The **RG.PYPROFILE STATS** command returns profiling statistics for a [session](glossary.html#session) id. Profiling information is automatically collected when [ProfileExecutions](configuration.md#profileexecutions) are enabled.

**Redis API**

```
RG.PYPROFILE STATS <session_id> [<order_by>]
```

_Arguments_

* _session_id_: the [session id](#rgpydumpsessions) to get the profiling statistics on.
* _order_by_: result ordering column, see [cProfile](https://docs.python.org/3.7/library/profile.html#pstats.Stats).

_Return_

String contains the collected profiling information.

**Output Example**

```
16 function calls in 2.003 seconds

   Ordered by: internal time

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        2    2.002    1.001    2.002    1.001 {built-in method time.sleep}
        4    0.000    0.000    0.000    0.000 <string>:289(profileStop)
        2    0.000    0.000    0.000    0.000 <string>:173(<lambda>)
        2    0.000    0.000    2.002    1.001 <string>:1(<lambda>)
        2    0.000    0.000    0.000    0.000 {built-in method builtins.__import__}
        4    0.000    0.000    0.000    0.000 {method 'disable' of '_lsprof.Profiler' objects}


   Ordered by: internal time

Function                                          was called by...
                                                      ncalls  tottime  cumtime
{built-in method time.sleep}                      <-       2    2.002    2.002  <string>:1(<lambda>)
<string>:289(profileStop)                         <-
<string>:173(<lambda>)                            <-
<string>:1(<lambda>)                              <-
{built-in method builtins.__import__}             <-       2    0.000    0.000  <string>:1(<lambda>)
{method 'disable' of '_lsprof.Profiler' objects}  <-       4    0.000    0.000  <string>:289(profileStop)


   Ordered by: internal time

Function                                          called...
                                                      ncalls  tottime  cumtime
{built-in method time.sleep}                      ->
<string>:289(profileStop)                         ->       4    0.000    0.000  {method 'disable' of '_lsprof.Profiler' objects}
<string>:173(<lambda>)                            ->
<string>:1(<lambda>)                              ->       2    0.000    0.000  {built-in method builtins.__import__}
                                                           2    2.002    2.002  {built-in method time.sleep}
{built-in method builtins.__import__}             ->
{method 'disable' of '_lsprof.Profiler' objects}  ->
```

## RG.PYPROFILE RESET
The **RG.PYPROFILE RESET** command resets profiling statistics for a given [session](glossary.html#session) id.

**Redis API**

```
RG.PYPROFILE RESET <session_id>
```

_Return_

A simple 'OK' string.

**Examples**

```
redis> RG.PYPROFILE RESET 0000000000000000000000000000000000000000-0
OK
```

## RG.PYDUMPREQS
The **RG.PYDUMPREQS** command returns a list of all the python requirements available (with information about each requirement).

**Redis API**

```
RG.PYDUMPREQS
```

_Return_

An array that consists of alternating key name and value entries as follows:

* **GearReqVersion**: an internally-assigned version of the requirement (note: this isn't the package's version)
* **Name**: the name of the requirement as it was given to the `REQUIREMENTS` argument of the  [RG.PYEXECUTE](commands.md#rgpyexecute) command
* **IsDownloaded**: `yes` if the requirement wheels was successfully download otherwise `no`
* **IsInstalled**: `yes` if the requirement wheels was successfully installed otherwise `false`
* **Wheels**: a list of wheels required by the requirement

**Examples**

```
127.0.0.1:6379> RG.PYDUMPREQS
1)  1) "GearReqVersion"
    2) (integer) 1
    3) "Name"
    4) "redis"
    5) "IsDownloaded"
    6) "yes"
    7) "IsInstalled"
    8) "yes"
    9) "Wheels"
   10) 1) "redis-3.5.2-py2.py3-none-any.whl"
```

## RG.REFRESHCLUSTER
The **RG.REFRESHCLUSTER** command refreshes the node's view of the cluster's topology.

!!! important "Open Source Redis Cluster"
    The `RG.REFRESHCLUSTER` command needs to be executed on each of the cluster's nodes.

**Redis API**

```
RG.REFRESHCLUSTER
```

_Return_

A simple 'OK' string.

**Examples**

```
redis> RG.REFRESHCLUSTER
OK
```

## RG.TRIGGER
The **RG.TRIGGER** command triggers the execution of a registered [CommandReader](readers.md#commandreader) function.

**Redis API**

```
RG.TRIGGER <trigger> [arg ...]
```

_Arguments_

* **trigger**: the trigger's name
* **arg**: any additional arguments

_Return_

An array containing the function's output records.

**Examples**

```
redis> RG.PYEXECUTE "GB('CommandReader').register(trigger='mytrigger')"
OK
redis> RG.TRIGGER mytrigger foo bar
1) "['mytrigger', 'foo', 'bar']"
```

## RG.TRIGGERONKEY
The **RG.TRIGGERONKEY** command is the same as **RG.TRIGGER**, the difference is that the third argument is considered a key so client will know how to direct the command to the correct shard.

**Redis API**

```
RG.TRIGGERONKEY <trigger> key [arg ...]
```

_Arguments_

* **trigger**: the trigger's name
* **key**: a key on which the trigger is executed on
* **arg**: any additional arguments

_Return_

An array containing the function's output records.

**Examples**

```
redis> RG.PYEXECUTE "GB('CommandReader').map(lambda x: execute('set', x[1], x[2])).register(trigger='my_set')"
OK
redis> RG.TRIGGERONKEY my_set foo bar
1) "OK"
redis> get foo
bar
```

## RG.UNREGISTER
The **RG.UNREGISTER** command removes the [registration](functions.md#registration) of a function.

**Redis API**

```
RG.UNREGISTER <id>
```

_Arguments_

* **id**: the [registration ID](functions.md#registration-id) for removal

_Return_

A simple 'OK' string, or an error. An error is returned if the registration ID doesn't exist or if the function's reader doesn't support the unregister operation.

## RG.CLEARREGISTRATIONSSTATS
The **RG.CLEARREGISTRATIONSSTATS** command clear stats from all the registrations, cleared stats:

* numTriggered
* numSuccess
* numFailures
* numAborted
* lastRunDurationMS
* avgRunDurationMS
* avgRunDurationMS
* lastEstimatedLagMS (on streams)
* avgEstimatedLagMS (on streams)

**Redis API**

```
RG.CLEARREGISTRATIONSSTATS
```

_Return_

A simple 'OK' string.

## RG.PAUSEREGISTRATIONS
The **RG.PAUSEREGISTRATIONS** command pause a given registrations from triggering any more events. **Currently its only possible to pause a stream registrations**. pause a registration that already pause is consider as no op and will keep the state exactly as it is (without any errors). pause is considered an atomic operation, all the registrations that was given will be pause together and if one failed, the entire operation is aborted (atomicity is promised on the shard level and not on the cluster level).

It is also possible to pause the registration from within the registration code itself by returning a special error message that starts with `PAUSE` string (message must be raised with [flatError](runtime.md#flat-error) api so the error trace will not be added to the error message), example:

```python
GB('StreamReader').foreach(lambda x: flatError('PAUSE registration is paused')).register()
```

**Redis API**

```
RG.PAUSEREGISTRATIONS id1 [id2 ...]
```

_Return_

A simple 'OK' string. Or error if operation failed.

## RG.UNPAUSEREGISTRATIONS
The **RG.UNPAUSEREGISTRATIONS** command unpause a given registrations and cause it to restart triggering events. **Currently its only possible to unpause a stream registrations**. Unpause a registration that already running is consider as no op and will keep the state exactly as it is (without any errors). Unpause is considered an atomic operation, all the registrations that was given will be unpause together and if one failed, the entire operation is aborted (atomicity is promised on the shard level and not on the cluster level). Unpausing a registration will restart processing data from the stream, if the stream is not set to trim messages (using trimStream option) then all the element will potentially be processed again.

**Redis API**

```
RG.UNPAUSEREGISTRATIONS id1 [id2 ...]
```

_Return_

A simple 'OK' string. Or error if operation failed.

