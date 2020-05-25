# RedisGears Commands

RedisGears is operated by commands sent to the server with a [Redis client](glossary.md#client).

The following sections describe the supported commands.

| Command | Description |
| --- | --- |
| [`RG.ABORTEXECUTION`](#rgabortexecution) | Aborts execution |
| [`RG.CONFIGGET`](#rgconfigget) | Returns configuration key |
| [`RG.CONFIGSET`](#rgconfigset) | Sets configuration key |
| [`RG.DROPEXECUTION`](#rgdropexecution) | Removes execution |
| [`RG.DUMPEXECUTIONS`](#rgdumpexecutions) | Outputs executions |
| [`RG.DUMPREGISTRATIONS`](#rgdumpregistrations) | Outputs registrations |
| [`RG.GETEXECUTION`](#rggetexecution) | Returns the details of an execution |
| [`RG.GETRESULTS`](#rggetresults) | Returns the results from an execution |
| [`RG.GETRESULTSBLOCKING`](#rggetresultsblocking) | Blocks client until execution ends |
| [`RG.INFOCLUSTER`](#rginfocluster) | Returns cluster information |
| [`RG.PYEXECUTE`](#rgpyexecute) | Executes a Python function |
| [`RG.PYSTATS`](#rgpystats) | Returns memory usage statistics |
| [`RG.PYDUMPREQS`](#rgpystats) | Returns detailed information about requirements |
| [`RG.REFRESHCLUSTER`](#rgrefreshcluster) | Refreshes node's view of the cluster |
| [`RG.TRIGGER`](#rgtrigger) | Triggers execution of registration |
| [`RG.UNREGISTER`](#rgunregister) | Removes registration |

**Syntax Conventions**

The following conventions are used for describing the RedisGears Redis API:

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

An array with an entry per key. The entry is a String being the value or an error for undefined options.

**Examples**

```
redis> RG.CONFIGGET ProfileExecutions
1) (integer) 0
redis> RG.CONFIGGET foo
1) (error) (error) Unsupported config parameter: foo
```

## RG.CONFIGSET
The **`RG.CONFIGGET`** command sets the value of one ore more built-in [configuration](configuration.md) or a user-defined options.

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

An array with an entry per execution. Each entry is made of alternating key name and value entries as follows:

* **executionId**: the [execution ID](functions.md#execution-id)
* **status**: the [status](functions.md#execution-status)

**Examples**

```
redis> RG.DUMPEXECUTIONS
1) 1) "executionId"
   2) "0000000000000000000000000000000000000000-0"
   3) "status"
   4) "done"
2) 1) "executionId"
   2) "0000000000000000000000000000000000000000-1"
   3) "status"
   4) "running"
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
RG.PYEXECUTE "<function>" [REQUIREMENTS "<dep> ..."] [UNBLOCKING]
```

_Arguments_

* _function_: the Python function
* _REQUIREMENTS_: this argument ensures that list of dependencies it is given as an argument is installed on each shard before execution
* _UNBLOCKING_: doesn't block the client during execution

_Return_

An error is returned if the function can't be parsed, as well as any that are generated by non-RedisGears functions used.

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
