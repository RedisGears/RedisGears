# RedisGears Commands

## RG.PYEXECUTE

### Format
```
  RG.PYEXECUTE {PYTHON-SCRIPT} [UNBLOCKING]
```

### Description
Running the given python script. The python script may or may not run Gears execution. If the script will not run Gears execution then it will only return a `OK` reply if the script was running successfully. If the script does create Gears execution then the results will be returned when the execution will finish, an exception is when using the UNBLOCKING flag, if used then the execution id will be return and it will be possible to track the execution results using  `RG.GETRESULTS` and `RG.GETRESULTSBLOCKING` (will be explained in details later). If error accured when running the script then the error will be return as the reply.

### Parameters

* **PYTHON-SCRIPT**: the python script to run.

* **UNBLOCKING**: run the script in an unblocking mode, the execution id will return as a result.

### Returns
The execution result, unless no execution was perform (in this case, on `OK` reply will be returned notify that the script was running successfully). If an error accure the error discription will be return.

---

## RG.GETRESULTS

### Format
```
  RG.GETRESULTS {EXECUTION-ID}
```

### Description
Returns the results of the given execution id. Return an error if the given execution does not exists or if the execution is still running.

### Parameters

* **EXECUTION-ID**: the exectuion id on which to return the results.

### Returns
Execution results or error.

---

## RG.GETRESULTSBLOCKING

### Format
```
  RG.GETRESULTSBLOCKING {EXECUTION-ID}
```

### Description
Same as `RG.GETRESULTS` but will block untill the excution will finished. Return error if the execution does not exists.

### Parameters

* **EXECUTION-ID**: the exectuion id on which to return the results.

### Returns
Execution results or error.

---

## RG.DUMPEXECUTIONS

### Format
```
  RG.DUMPEXECUTIONS
```

### Description
Return all the executions including the following information:

* executionId
* status (running/done)

### Returns
List of executions

---

## RG.DROPEXECUTION

### Format
```
  RG.DROPEXECUTION EXECUTION-ID
```

### Description
Dropping the given execution id, return error if the execution does not exists or is still running.


### Parameters

* **EXECUTION-ID**: the exectuion id to drope.


### Returns
OK on success or error.

---