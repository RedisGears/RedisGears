# Commands

## TFUNCTION LOAD

Load a new library to RedisGears.

```
TFUNCTION LOAD [REPLACE] [CONFIG <config>] "<library code>"
```

_Arguments_

* REPLACE - an optional argument, instructs RedisGears to replace the function if its already exists.
* CONFIG - a string representation of a JSON object that will be provided to the library on load time, for more information refer to [library configuration](function_advance_topics.md#library-configuration)
* _library code_ - the library code

_Return_

An error, if the loading failed or "OK" if everything was done correctly.

**Example**
```bash
> TFUNCTION LOAD "#!js api_version=1.0 name=lib\n redis.register_function('foo', ()=>{return 'bar'})"
OK
```

## TFUNCTION DELETE

Delete a library from RedisGears.

```
TFUNCTION DELETE "<library name>"
```

_Arguments_

* _library name_ - the name of the library to delete

_Return_

An error, if the library does not exists or "OK" if the library was deleted successfully.

**Example**
```bash
> TFUNCTION DEL lib
OK
```

## TFUNCTION LIST

List the functions with additional information about each function.

```
TFUNCTION LIST [WITHCODE] [VERBOSE] [v] [LIBRARY <library name>]
```

_Arguments_

* WITHCODE - Show libraries code.
* VERBOSE - Increase output verbosity (can be used multiple times to increase verbosity level).
* v - Same as VERBOSE
* LIBRARY - Optional argument allow specifying a library name (can be used multiple times to show multiple libraries in a single command)

_Return_

Information about the requested libraries.

**Example**
```bash
> TFUNCTION list vvv
1)  1) "engine"
    2) "js"
    3) "api_version"
    4) "1.0"
    5) "name"
    6) "lib"
    7) "pending_jobs"
    8) (integer) 0
    9) "user"
    10) "default"
    11) "functions"
   12) 1) 1) "name"
          2) "foo"
          3) "flags"
          4) (empty array)
   13) "stream_consumers"
   14) (empty array)
   15) "notifications_consumers"
   16) (empty array)
   17) "gears_box_info"
   18) (nil)

```

## TFCALL

Invoke a function.

```
RFCALL <library name> <function name> <number of keys> [<key1> ... <keyn>] [<arg1> ... <argn>]
```

_Arguments_

* _library name_ - The library name contains the function.
* _function name_ - The function name to run.
* _number of keys_ - The number of keys that will follow
* _key1_ ... _keyn_ - keys that will be touched by the function.
* _arg1_ ... _argn_ - Additional argument to pass to the function.

_Return_

The return value from the function on error in case of failure.

**Example**
```bash
> TFCALL lib foo 0
"bar"
```

# TFCALLASYNC

Invoke an async function (Coroutine).

```
TFCALLASYNC <library name> <function name> <number of keys> [<key1> ... <keyn>] [<arg1> ... <argn>]
```

_Arguments_

* _library name_ - The library name contains the function.
* _function name_ - The function name to run.
* _number of keys_ - The number of keys that will follow
* _key1_ ... _keyn_ - keys that will be touched by the function.
* _arg1_ ... _argn_ - Additional argument to pass to the function.

_Return_

The return value from the async function on error in case of failure.

**Example**
```bash
> TFCALLASYNC lib foo 0
"bar"
```
