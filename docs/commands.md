# Commands

## RG.FUNCTION LOAD

Load a new library to RedisGears.

```
RG.FUNCTION LOAD [UPGRADE] "<library code>"
```

_Arguments_

* UPGRADE - an optional argument, instructs RedisGears to upgrade the function if its already exists.
* _library code_ - the library code

_Return_

An error, if the loading failed or "OK" if everything was done correctly.

**Example**
```bash
> RG.FUNCTION LOAD "#!js name=lib\n redis.register_function('foo', ()=>{return 'bar'})"
OK
```

## RG.FUNCTION DEL

Delete a library from RedisGears.

```
RG.FUNCTION DEL "<library name>"
```

_Arguments_

* UPGRADE - an optional argument, instructs RedisGears to upgrade the function if its already exists.
* _library name_ - the name of the library to delete

_Return_

An error, if the library does not exists or "OK" if the library was deleted successfully.

**Example**
```bash
> RG.FUNCTION DEL lib
OK
```

## RG.FUNCTION LIST

List the functions with additional information about each function.

```
RG.FUNCTION LIST [WITHCODE] [VERBOSE] [v] [LIBRARY <library name>]
```

_Arguments_

* WITHCODE - Show libraries code.
* VERBOSE - Increase output verbosity (can be used mutiple times to increase verbosity level).
* v - Same as VERBOSE
* LIBRARY - Optional argument allow specifying a library name (can be used multiple times to show multiple libraries in a single command)

_Return_

Information about the requested libraries.

**Example**
```bash
> RG.FUNCTION list vvv
1)  1) "engine"
    2) "js"
    3) "name"
    4) "lib"
    5) "pending_jobs"
    6) (integer) 0
    7) "user"
    8) "default"
    9) "functions"
   10) 1) 1) "name"
          2) "foo"
          3) "flags"
          4) (empty array)
   11) "stream_consumers"
   12) (empty array)
   13) "notifications_consumers"
   14) (empty array)
   15) "gears_box_info"
   16) (nil)

```

## RG.FUNCTION CALL

Invoke a function.

```
RG.FUNCTION CALL <library name> <function name> [<arg1> ... <argn>]
```

_Arguments_

* _library name_ - The library name contains the function.
* _function name_ - The function name to run.
* _arg1_ ... _argn_ - Additional argument to pass to the function.

_Return_

The return value from the function on error in case of failure.

**Example**
```bash
> RG.FUNCTION CALL lib foo
"bar"
```
