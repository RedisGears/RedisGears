# RedisGears Design: Python Sub-Interpreters
RedisGears ships with an embedded [Python Interpreter](runtime.md#python-interpreter). This makes it possible to run Python code using the [`RG.PYEXECUTE` command](commands.md#rgpyexecute). Because the interpreter is a singleton that's shared among all calls to `RG.PYEXECUTE`, there's the risk of different executions using the same identifiers (e.g. global variables, functions or class names).

One way to address this problem is by restarting the interpreter before each execution. However, an interpreter restart is a time-costly operation that renders this approach less-than-desirable. Also, two executions may be parallel, either in two different threads or even in the same thread (while the execution of the Python code itself is always non-parallelized, the rest of the execution's lifecycle may be). When more than one execution is running, restarting the interpreter is no longer an option. To address the issue correctly RedisGears make use of [Python's Sub-Interpreters](https://docs.python.org/3/c-api/init.html#sub-interpreter-support).

A sub-interpreter is a (almost) separate environment for the execution of Python code. The Python C API makes it possible to create a new sub-interpreter using `Py_NewInterpreter`, destroy it using `Py_EndInterpreter` and switch between sub-interpreters using `PyThreadState_Swap`. RedisGears invokes these internally and maintains the association between the user's call to `RG.PYEXECUTE` and its respective sub-interpreter.

When `RG.PYEXECUTE` is called, a new sub-interpreter is created to execute the provided script. That sub-interpreter is also "inherited" by all subsequent operations - i.e. executions, registrations, and time events, that the script creates. Because there may be multiple owners of the sub-interpreter, RedisGears keeps an internal reference count for each one so it can be safely freed.

Notice that the isolation between Sub-Interpreters isn’t absolute. For example, when using low-level file operations like `os.close()` they can (accidentally or maliciously) affect each other’s open files because of the way extensions are shared between sub-interpreters.

!!! info "Further reference"
    * [Python Sub-Interpreters](https://docs.python.org/3/c-api/init.html)
