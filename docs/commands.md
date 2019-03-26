# RedisGears Commands

## Execution Response
RedisGears allows you to run python scripts.  These python scripts can run in a `Gears` and a `non Gears` mode.  The execution result depends on the type of execution and success or failure.

* When running a `Gears` execution
	- successful: the execution result
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

* the execution result (see above)


## RG.GETRESULTS
```sql
RG.GETRESULTS execution-id
```
* execution-id: the exectuion id for which to return the results.

Returns the results of the given execution id. Returns an error if the given execution does not exists or if the execution is still running.

### Returns
Execution result (see above) or an error when:

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
Execution result (see above) or an error when:

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


## RG.DROPEXECUTION
```sql
RG.DROPEXECUTION execution-id
```
* execution-id: the execution id for which to drop the execution result.

Dropping the given execution-id result, returns error if the execution does not exists or is still running.

### Returns
OK on success or error when:

* the script is still running
* if the execution-id doesn't exist or is no longer available (see configuration, we keep by default the last 1000 results)
