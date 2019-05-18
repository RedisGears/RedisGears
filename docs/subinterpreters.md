# Sub-Interpreters
RedisGears support full embeded python interpreter. It is possible to run python code inside Redis using `RG.PYEXECUTE`. The problem with this is that two `RG.PYEXECUTE` commands will actually share the same interpreter (they will see the same globals variable, function, classes, ...) and they might step on each other fit. One way to overcome this problem is to shut down and restart the interpreter in each execution, the problem with this solution is that it requires time to restart the interpreter, doing this over and over again is definitely not cheap. In addition it is possible for two execution to run simultaneously, in two different thread or even on the same thread (its true that the python code itself can not run simultaneously, but the rest of the execution lifecycle might). In order to solve this issue correctly RedisGears make use of Sub-interpreters.

Sub-interpreter is an (almost) totally separate environment for the execution of Python code. It is possible to create a new Sub-interpreter using `Py_NewInterpreter` and destroy it using `Py_EndInterpreter`. It is also possible to switch between Sub-interpreter using `PyThreadState_Swap` (just to avoid any confusion, those function are C api of the python interpreter, RedisGears use them behind the scene and those are not expose to the user in any way).

It is possible for RedisGears to create a new Sub-interpreter each time it runs a python code, i.e each time it envoke any callback or time event function that was supplied by the user. There are two problems with this approach:

* Creating a subinterpreter is not cheap (unlike switching to an existing subinterpreter which is much cheaper).
* We lose the context between callbacks, i.e if the callbacks uses some global variable, its value (and its actual existance) will not be mainted between two function call. It will not be possible to do the following (when we call incrA from the execution thread the `a` variable will not be defined):
```
a = 1
def incrA(x):
	global a
	a += 1
GB().map(incrA).register()
```

So in order to achieve correct balance between run isolation and still allow using globals variables/functions we decided that Sub-Interpreters should be shared between the actual python script and the executions/registretions/timeEvents that it creates. When user performs `RG.PYEXECUTE` we create a new Sub-Interpreter to execute the given script. All the execution that will be triggered and all the timeEvent that this script will create will run on this same Sub-Interpreter. So now Sub-Interpreter might has multiple owner which means that we must use ref count in order to know when it safe to free it. We wrapped the Sub-Interpreter with a struct that contains ref count. Each time we create an execution inside a python script, the execution shared ownership of the Sub-Interpreter by increasing its ref count. Each time we run a python code as part of an execution (a map callback for example) we switch to the relevant Sub-Interpreter. The same thing happeneds on TimeEvents. On dropping execution/TimeEvent we decrease the Sub-Interpreter ref count. When Sub-Interpreter ref count reaches zero, we free the Sub-Interpreter.

Notice that insulation between Sub-Interpreters isn’t perfect — for example, using low-level file operations like os.close() they can (accidentally or maliciously) affect each other’s open files. Because of the way extensions are shared between interpreters. For more information, read [Sub-interpreters](https://docs.python.org/3/c-api/init.html) documentation, make sure you are not doing something that might break the Sub-Interpreters insulation.