# RedisGears Commands

## Execution Result
RedisGears allows you to run python scripts.  These python scripts can run in a `Gears` and a `non Gears` mode.  The execution result depends on the type of execution and success or failure.

* When running a `Gears` execution
	- successful: the results and errors (if any) as an array consisting of two arrays
	- failure: the error message
* When running a `non Gears` execution
	- successful: `OK`
	- failure: the error message


## RG.PYEXECUTE
```sql
RG.PYEXECUTE python-script [UNBLOCKING]
```
* python-script: the python script to run (string)
Optional args:
   * UNBLOCKING: run the script in an unblocking mode, in this case an execution ID is returned.  If ran in unblocking mode you can get your result with `RG.GETRESULTSBLOCKING`

Run the given python script. The python script may or may not run a `Gears` execution.

### Returns
When running in UNBLOCKING mode

* an execution ID.  You can get the result with `RG.GETRESULTSBLOCKING` or `RG.GETRESULTS`

When running in normal mode

* The [Execution Result](#execution-result)


## RG.GETRESULTS
```sql
RG.GETRESULTS execution-id
```
* execution-id: the exectuion id for which to return the results.

Returns the results of the given execution id. Returns an error if the given execution does not exists or if the execution is still running.

### Returns
The [Execution Result](#execution-result) or an error when:

* the script is still running
* if the script doesn't exist
* the execution result is no longer available (see configuration, we keep by default the last 1000 results)


## RG.GETRESULTSBLOCKING
```sql
RG.GETRESULTSBLOCKING execution-id
```
* execution-id: the execution id for which to return the results.

Same as `RG.GETRESULTS` but will block untill the execution is finished in which case it will return the Execution Result.

### Returns
The [Execution Result](#execution-result) or an error when:

* the execution-id doesn't exist
* execution is no longer available (see configuration, we keep by default the last 1000 results)


## RG.DUMPEXECUTIONS
```sql
RG.DUMPEXECUTIONS
```

Return all the executions.

### Returns
List of executions with
* executionId
* status (running/done)

## RG.GETEXECUTION
```sql
RG.GETEXECUTION execution-id [ SHARD | CLUSTER ]
```

Return an execution plan.

Optional args:
* `SHARD` - returns the local execution plan. This is the default for single-instance deployments.
* `CLUSTER` - returns the execution plan from all shards in the cluster. This is the default for cluster deployments.

Trivia: when called with the `CLUSTER` subcommand, RedisGears actually runs a gear that collects the shards' execution plans... self reference is so paradoxical :)

### Returns
An array where each entry represents a shard. A shard's entry is structured to mimic a a Hash (i.e. alternating name and value entries) with the following data:
* shard_id (string) - the shard's ID
* execution_plan (Hash as array) - the exection plan

Each shard's execution plan consists of the following:
* status (string) - the execution plan's status
* shards_received (integer) - the number of shards that received the plan from this shard
* shards_completed (integer) - the number of shards that had completed plan received from this shard
* results (integer) - the number of results collected (-1 if status is not done)
* errors (array) - the errors reported by the steps (empty array if not done)
* total_duration (integer) - the total duration of the plan in milliseconds
* read_duration (integer) - the duration of the plan's read stage in milliseconds
* steps (array) - the plan's steps

A note about durations: the total duration is greater than the sum of step durations due to the execution's overheads.

The steps array is made up of step entries. Each consists of the following:
* type (string) - the step's type
* duration (integer) - the step's execution duration in millisecondss
* name (string) - the step's name, if available
* arg (string) - the step's argument, if available

## RG.DROPEXECUTION
```sql
RG.DROPEXECUTION execution-id
```
* execution-id: the execution id for which to drop the execution result.

Dropping the given execution-id result, returns error if the execution does not exists or is still running.

### Returns
`OK` on success or error when:

* the script is still running
* if the execution-id doesn't exist or is no longer available (see configuration, we keep by default the last 1000 results)

## RG.DUMPREGISTRATIONS
```sql
RG.DUMPREGISTRATIONS
```

Return all registered executions.

### Returns
List of registered executions:
* registrationId
* execution reader
* execution description

## RG.UNREGISTER
```sql
RG.UNREGISTER registrationId
```

* registrationId: the registrationId to drop.

### Returns
`OK` on success or error when:
* id not exists
* the reader does not support unregister functionality
