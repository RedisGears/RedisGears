# Introduction to RedisGears

## What is RedisGears?
RedisGears is a serverless engine for transaction, [batch](glossary.md#batch-processing) and [event-driven](glossary.md#event-processing) data processing in Redis. It is a dynamic framework for the execution of [functions](functions.md) that, in turn, implement data flows in Redis, while (almost) entirely abstracting the data's distribution and choice of deployment  (i.e. stand-alone vs. cluster, OSS vs. Enterprise). Functions can be implemented in different languages, including Python and C [APIs](glossary.md#api).

That's a lot of high-level buzzes, so to explain what RedisGears does this introduction will take you from parking to 6th-gear.

!!! important "Prerequisites"
    Before diving into RedisGears please make sure that you are familiar with the basic concepts of Redis and Python.

## Getting Started
RedisGears is a Redis module, so it requires a [Redis](https://redis.io) server to run. The easiest way to get a standalone Redis server with RedisGears bootstrapped locally is to use the official RedisGears Docker container image:

```
docker run -d --name redisgears -p 6379:6379 redislabs/redisgears:latest
```

??? note "Further reference"
    For more information on installing RedisGears refer to the [Quickstart page](quickstart.md).

## Overview
RedisGears runs as a module inside a Redis server and is operated via a set of [Redis commands](commands.md). At the module's core is an engine that executes user-provided flows, or functions, through a programmable interface.

Functions can be executed by the engine in an ad-hoc batch-like fashion, or triggered by different events for event-driven processing. The data stored in the database can be read and written by functions, and a built-in coordinator facilitates processing distributed data in a cluster.

In broad strokes, the following diagram depicts RedisGears' components:

```
    +---------------------------------------------------------------------+
    | Redis Server               +--------------------------------------+ |
    |                            | RedisGears Module                    | |
    | +----------------+         |                                      | |
    | | Data           | Input   | +------------+ +-------------------+ | |
    | |                +-------->+ | Function   | | APIs              | | |
    | | Key1 : Value1  |         | | +--------+ | | C, Python, ...    | | |
    | | Key2 : Value2  | Output  | | | Reader | | +-------------------+ | |
    | | Key3 : Value3  <---------+ | +---+----+ | +-------------------+ | |
    | |      ...       |         | |     v      | | Redis commands    | | |
    | +----------------+         | | +---+----+ | | Gears admin & ops | | |
    |                            | | | Step 1 | | +-------------------+ | |
    |                            | | +---+----+ | +-------------------+ | |
    | +----------------+         | |     v      | | Coordinator       | | |
    | | Events         |         | | +---+----+ | | Cluster MapReduce | | |
    | |                | Trigger | | | Step 2 | | +-------------------+ | |
    | | Data update    +-------->+ | +---+----+ | +-------------------+ | |
    | | Stream message |         | |     v      | | Engine            | | |
    | | Time interval  |         | |    ...     | | Runtime execution | | |
    | |      ...       |         | +------------+ +-------------------+ | |
    | +----------------+         +--------------------------------------+ |
    +---------------------------------------------------------------------+
```

## The Simplest Example
Let us start by writing and executing the simplest possible RedisGears function. As a prerequisite, any communication with RedisGears requires using its commands via a Redis client, so we'll begin by using the official [`redis-cli`](https://redis.io/topics/rediscli). If you have it locally installed feel free to use that, but it is also available from the container:

```
docker exec -it redisgears redis-cli
```

Once at cli's the prompt, type in the following and then hit the `<ENTER>` on your keyboard to execute it:

```
RG.PYEXECUTE "GearsBuilder().run()"
```

!!! example "Example: executing the simplest function in redis-cli:"
    ```
    $ docker exec -it redisgears redis-cli
    127.0.0.1:6379> RG.PYEXECUTE "GearsBuilder().run()"
    1) (empty array)
    2) (empty array)
    ```

**Congratulations** - you've executed your first RedisGears function! But what had happened here?

## Functions
The first thing we've done was call the [**`RG.PYEXECUTE`**](commands.md#rgpyexecute) command. All of RedisGears' Redis commands are prefixed with `RG.`, and `RG.PYEXECUTE`'s purpose is, well, to execute a RedisGears function in Python.

The function is passed to the command as an argument that's enclosed in double-quotes. RedisGears functions in Python always begin with a context builder - the [**`#!python class GearsBuilder`**](functions.md#context-builder) - followed by the data flow's operations, and ending with an action. That means that functions generally look like this:

```
                                      +------------+
                                      | Function   |
                    +-------------+   | +--------+ |
                    | Input data  +-->+ | Reader | |
                    +-------------+   | +---+----+ |
                                      |     v      |
                                      | +---+----+ |
                                      | | Step 1 | |
                                      | +---+----+ |
                                      |     |      |
                                      |    ...     |
                                      |     v      |
                                      | +---+----+ |
                                      | | Step n | |
                                      | +---+----+ |
                                      |     v      |
                    +-------------+   | +---+----+ |
                    | Results     +<--+ | Action | |
                    +-------------+   | +--------+ |
                                      +------------+
```

In our simple example, the function performs no operations so it has no additional steps. It ends with the [**`run()`**](functions.md#run) action that causes the function to execute once and return. This type of execution is also referred to as batch and means that execution is started immediately on existing data.

Once the command is sent from the client (`redis-cli`) to the server, the engine executes the function and returns the reply. The reply consists of two arrays, both of which are empty in this case. The first array contains the function's output and the second array is reserved for reporting errors.

## Input Data
The RedisGears function we've executed had replied with an empty results array because it had no input to process. The initial input to any RedisGears function can be zero, one or more records that are generated by a reader.

A [**Record**](glossary.md#record) is the basic RedisGears abstraction that represents data in the function's flow. Input data records are passed from one step to the next and are finally returned as the result.

A [**Reader**](glossary.md#reader) is the mandatory first step of any function, and every function has exactly one reader. A reader reads data and generates input records from it. The input records are consumed by the function.

There are several [reader types](readers.md) that the engine offers. A function's reader type is always declared during the initialization of its `GearsBuilder()` context. Unless explicitly declared, a function's reader defaults to the [KeysReader](readers.md#keysreader), meaning the following lines are interchangeable:

!!! example "Example: three ways for achieving the same"
    ```python
    {{ include('intro/intro-000.py') | indent(4) }}
    ```

The [**KeysReader**](readers.md#keysreader) scans the Redis database and generates records made of the keys found and their respective values. Let's add some data to Redis to see how that changes things. First we'll create a key called "foo" with a String value of "bar" using the Redis [`SET` command](https://redis.io/commands/set):

```
SET foo bar
```

Once that's done, we'll run the simple function again (you can use the arrow keys for scrolling history). Your terminal should look like this:

!!! example "Example: adding datum"
    ```
    127.0.0.1:6379> SET foo bar
    OK
    127.0.0.1:6379> RG.PYEXECUTE "GearsBuilder().run()"
    1) 1) "{'key': 'foo', 'value': 'bar'}"
    2) (empty list or set)
    ```

The function's results array now contains a single record, generated by the KeysReader, which represents the key we had just created and its value. Let's add a couple of Hashes to represent fictitious personas. Execute these Redis commands:

```
HSET person:1 name "Rick Sanchez" age 70
HSET person:2 name "Morty Smith" age 14
```

Then, run the function again.

!!! example "Example: adding more data"
    ```
    127.0.0.1:6379> HSET person:1 name "Rick Sanchez" age 70
    (integer) 2
    127.0.0.1:6379> HSET person:2 name "Morty Smith" age 14
    (integer) 2
    127.0.0.1:6379> RG.PYEXECUTE "GearsBuilder().run()"
    1) 1) "{'key': 'foo', 'value': 'bar'}"
       2) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       3) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
    2) (empty list or set)
    ```

!!! note "Order of reply"
    The order of elements in the reply may be different.

Now that there are three keys in the database, the function returns three result records, one for each. Note how the type of 'value' field differs between the String and Hash records - the former is a string and the latter a dictionary.

The following visualization summarizes what we've achieved so far:

```
          Data                               Python Function
          +----------+-------------------+   +----------------+
          | Key      | Value             |   | GearsBuilder() |
          +------------------------------+   |                |
          | foo      | bar               +--------+ Key:Value |
          | person:1 | {name: Rick ...}  |   |    v           |
          | person:2 | {name: Morty ...} |   | +--+---------+ |
          +----------+-------------------+   | | KeysReader | |
                                             | +--+---------+ |
          Results                            |    |           |
          +------------------------------+   |    | Record    |
          |{key: foo, value: bar }       |   |    v           |
          +------------------------------+   | +--+---------+ |
          |{key: person:1, value: {...}} +<----+ run()      | |
          +------------------------------+   | +------------+ |
          |{key: person:2, value: {...}} |   |                |
          +------------------------------+   +----------------+
```

## Keys Pattern
By default, the KeysReader reads all keys in the database. This behavior can be controlled by providing the reader with a glob-like pattern that, upon the function's execution, is matched against every key name. The reader generates input records only for the keys with names that successfully match the pattern.

The reader's key names' pattern is set to "*" by default, so any key name matches it. One way to override the default pattern is from the context's `run()` method. To have input records consisting only of persons, we can use the pattern `person:*` to discard keys that don't match it by providing it like so:

```
RG.PYEXECUTE "GearsBuilder().run('person:*')"
```

Running the function with the pattern should result as follows:

!!! example "Example: using a key pattern"
    ```
    127.0.0.1:6379> RG.PYEXECUTE "GearsBuilder().run('person:*')"
    1) 1) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       2) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
    2) (empty list or set)
    ```

The reply now consists only of those records that had matched the key name pattern, effectively excluding the key "foo" from our function's input.

## Flow Steps
Data is now flowing into our function, so it can be processed. RedisGears functions describe steps in a data processing flow that always begins with a reader. The reader can generate any number of input records as its output. These records are used as input for the next step in the flow, in which the records can be operated upon in some manner and then output. Multiple steps can be added to the flow, with each transforming its input records in some meaningful way to one or more output records.

To see how this works in practice, we'll refactor our function to use a [**`filter()`**](operations.md#filter) operation as a step instead of the reader's keys pattern:

!!! example "Example: `filter()` operation"
    ```
    127.0.0.1:6379> RG.PYEXECUTE "GB().filter(lambda x: x['key'].startswith('person:')).run()"
    1) 1) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       2) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
    2) (empty list or set)
    ```

Although the results appear identical to the previous execution, they were obtained differently. Here's the same function, only formatted for ease of reading:

```python
GB() \
  .filter(lambda x: x['key'].startswith('person:')) \
  .run()
```

The first change to our function is purely syntactical and replaces the verbose form of the function's context constructor with a shorter alias.

??? tip "`GB()` is an alias for `GearsBuilder()`"
    It is intended to be used for brevity, increased productivity and the reduction of finger strain due to repetitive typing.

The next change to the flow is much more significant. It adds a new operation between the function's context initialization and execution. A `filter()` operation, as the name suggests, filters its input. This is done by evaluating each record against the provided function. Only records for which `filter()`'s function returns a `True` value are output (as input) to the next step.

??? note "Lambda and regular function callbacks"
    The example above uses a Python lambda function as the argument to the `filter()` step, but that's hardly a requirement. Traditional Python callbacks (functions are 1st-class citizens) are perfectly ok too, so you can implement the same flow with a regular callback:

    ```python
    {{ include('intro/intro-001.py') | indent(4) }}
    ```

The `filter()` operation invokes the filtering function once for every input record it gets. The input record denoted as `x` in the examples, is a dictionary in our case and the function checks whether the value of its `key` key conforms to the requested pattern.

The main difference between the function that uses the reader's key pattern and the one using the step is in time when the filtering act happens. In the key pattern's case, filtering is done by the reader itself - after it obtains the keys' names but before reading their values. Conversely, with the `filter()` operation in the flow, the reader reads all keys (and their values), that are only then turned to records and filtered by the step.

Functions can be as complex as needed and can consist of any number of steps that are executed sequentially. Furthermore, RedisGears Python API allows the use of all of the language's features to be used, including importing and using external packages.

## Executing Complex Functions
Our simple RedisGears function is hardly "complex" yet, but typing it into the prompt is already becoming tiresome. Furthermore, as you may have found out, `redis-cli`'s interactive mode (a.k.a REPL) is not really suited for multi-line text editing.

Instead of using the interactive mode, you can store your functions' code in a regular text file and have the `redis-cli` client send its contents for execution. For example, if you'll save your function in a local file called "mygear.py" and you're using the `redis-cli` from the Docker container, then you can execute it with:

```
cat mygear.py | docker exec -i redisgears redis-cli -x RG.PYEXECUTE
```

## Processing Data
We saw how input records are read and then filtered using a step, but that's literally just the beginning. By adding more steps to the function, we can manipulate the data in any way needed using different operations and the language's capabilities.

To see how this works in practice, we'll gradually extend our function until it can be used to compute the maximum age of persons in the database.

## Mapping Records
All we care about now are persons' ages, so we'll start by transforming the records to strip them from all other data. Transforming a record from one shape to another is referred to as mapping operation and the [**`map()`**](operations.md#map) operation implements it:

```python
{{ include('intro/intro-002.py') }}
```

Exactly like `filter()`, the `map()` operation accepts a single function callback argument. The step executes the mapping function once on each of its input records, and whatever the function returns becomes an output record for the next step. In our example, the mapping function transforms the record's value dictionary into a single numeric value by extracting, casting and returning the value of the "age" key from the value of the record's "value" key.

When you run the function with the mapping step, the results should be:

!!! example "Example: `map()` operation"
    ```
    $ cat mygear.py | docker exec -i redisgears redis-cli --no-raw -x RG.PYEXECUTE
    1) 1) "70"
       2) "14"
    2) (empty list or set)
    ```

## Accumulating Data
Computing the maximum age from our records is done by iterating on all of them and picking the highest value. RedisGears supports this type of processing with the [**`accumulate()`**](operations.md#accumulate) step. This step groups multiple input records, usually for summation purposes, and follows this pattern:

1. Initialize an accumulator variable to the zero value
2. For each input record, manipulate the accumulator's value accordingly
3. Output the accumulator

??? tip "Important: use `aggregate()` instead of `accumulate()`"
    We'll be using the `accumulate()` operation to demonstrate basic aggregation. Generally speaking, you should use the [**`aggregate()`**](operations.md#aggregate) for computing aggregates that also accounts for the distribution of data.

    These concepts are explained later in the [Distributed Processing](#distributed-processing) section.

So, in our case by following these steps, to compute the maximal age we'll need to:

1. Set the accumulator to 0
2. Compare each records' value to the accumulator - when greater, update it
3. Output the maximum value that the accumulator stores as a record

We'll implement this with a function - `maximum()` - that we'll provide to the `accumulate()` step as an argument:

```python
{{ include('intro/intro-003.py') }}
```

The most noticeable thing about the accumulating function is that, unlike the functions used by `filter()` and `map()` that operate on a single record argument, it accepts two arguments: an accumulator (argument `a`) and an input record (`x`). This allows the accumulator to be carried from between executions of accumulating function on different records.

The accumulator is initialized by RedisGears to a Pythonic `None`, so the function's first instruction initializes it to 0. Then, the record is compared to the accumulator's value and the latter is updated if needed. The `maximum()` function ends by returning the accumulator.

??? tip "Simple aggregates"
    By using different accumulating functions you can compute other simple aggregates. For example, you can use the following function to count records:

    ```python
    {{ include('intro/intro-004.py') | indent(4) }}
    ```

## Aggregating Data
We've seen how accumulating data allows a RedisGears function to calculate simple aggregates such as maximum and count. That pattern is also used for computing more complex ones, such as the average person age for example.

??? tip "The `avg()` operation"
    The RedisGears Python API includes the [**`avg()`**](operations.md#avg) operation that you can always use instead of coding your own.

To compute the average age from the data in our database, we'll need two accumulators: one for summing the records' values, and another one for counting them. After we've iterated all records to obtain these accumulated values, we want to output the quotient that results from their division.

In more abstract terms, we'll implement a pattern that looks like this:
1. Initialize two accumulator variables - one for the sum of ages and the other for their count - to their respective zero values
2. For each record, add the value to the sum accumulator and increase the counter by one.
3. Output the result of dividing the sum and count accumulators

Here's how the first two steps in the aggregate flow are achieved with RedisGears by defining and calling the `prepare_avg()` function from an `accumulate()` flow step:

```python
{{ include('intro/intro-005.py') }}
```

Instead of using a single value for the accumulator, we opt for a Pythonic tuple in which the first element represents the sum of ages, and the second element their count. After all records have been processed, and in to derive the average from the function's output tuple, we can add a final `map()` operation that calls `compute_avg()` to the flow:

```python
{{ include('intro/intro-006.py') }}
```

## Blocking vs. Nonblocking Execution
The time it takes to execute a function depends on both its input and its complexity. RedisGears executes batch functions asynchronously in a thread running in the background, thus allowing the main Redis process to continue serving requests while the engine is processing.

The default behavior for `RG.PYEXECUTE` is to block the client that had called. A blocked client waits for the server's reply before continuing, and in the case of a RedisGears function, that means until processing is complete. Then, any results generated are returned to the client and it is unblocked.

Blocking greatly simplifies the client's logic, but for long-running tasks, it is sometimes desired to have the client continue its work while the function is executed. RedisGears batch functions can be executed in this non-client-blocking mode by adding the `UNBLOCKING` argument to the `RG.PYEXECUTE` command. For example, we can run the first version of our simple function in a nonblocking fashion like so:

!!! example "Example: running a nonblocking function"
    ```
    127.0.0.1:6379> RG.PYEXECUTE "GB().run()" UNBLOCKING
    "0000000000000000000000000000000000000000-0"
    ```

When executing in `UNBLOCKING` mode, the engine replies with an [**Execution ID**](functions.md#execution-id) that represents the function's execution internally. The execution IDs are unique. They are made of two parts, a shard identifier and a sequence, that are delimited by a hyphen ('-'). The shard identifier is unique for each shard in a Redis Cluster, whereas the sequence is incremented each time the engine executes a function.

By calling the [**`RG.DUMPEXECUTIONS`** command](commands.md#rgdumpexecutions), we can fetch the engine's executions list, which currently has just one entry representing the function we've just run:

!!! example "Example: dumping executions"
    ```
    127.0.0.1:6379> RG.DUMPEXECUTIONS
    1) 1) "executionId"
       2) "0000000000000000000000000000000000000000-0"
       3) "status"
       4) "done"
    ```

Because the function's execution is finished, as indicated by the value "done" of the "status" field, we can now obtain its execution results with the [**`RG.GETRESULTS`** command](commands.md#rggetresults). As the name suggests, the command returns the results of the execution specified by its ID:

!!! example "Example: getting results of a nonblocking execution"
    ```
    127.0.0.1:6379> RG.GETRESULTS 0000000000000000000000000000000000000000-0
    1) 1) "{'key': 'foo', 'value': 'bar'}"
       2) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       3) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
    2) (empty list or set)
    ```

Had we called `RG.GETRESULTS` before the execution was "done", the engine would have replied with an error. A client that opts for nonblocking execution can use that to poll for results or continue working in case an error was returned.

Lastly, if it has no work left to done, a client can return to blocking mode by calling the [**`RG.GETRESULTSBLOCKING`** command](commands.md#rggetresultsblocking). Calling `RG.GETRESULTSBLOCKING` blocks the client until the execution is finished, whereupon the client is unblocked with the results.

!!! note
    Functions that are executed in blocking mode are not added to RedisGears' executions and results lists.

## Event Processing
Until this point we've executed batch functions, which means that we've used the `run()` action to have the function execute immediately. When executed in this fashion, the function's reader fetches whatever data there is and then stops. Once the reader stops, the function is finished and its results are returned.

In many cases, data constantly changes and needs to be processed in an event-driven manner. For that purpose, RedisGears functions can be registered as triggers that "fire" on specific events to implement what is known as stream processing flows. A registered function's reader doesn't read existing data but rather waits for new input to trigger steps instead.

When registered to process streaming data, the function is executed once for each new input record as a default. Whereas batch functions are executed exactly once, a registered function's execution may be triggered any number of times in response to the events that drive it.

To try this, we'll return to the maximum computing example and have it executed in response to new data with the [`register()`](functions.md#register) action:

```python
{{ include('intro/intro-007.py') }}
```

By ending a function with the [`register()`](functions.md#register) action and sending it to RedisGears, the engine registers it and will execute it in response to the reader's events. In the case of the **KeysReader**, events are generated every time keys that match the pattern `person:*` are written to the database.

Just `register()`ing the function doesn't trigger its execution, and the "OK" returned in the response serves only to indicate that its registration was successful. Changes to data will trigger execution, which can be done for example with the following Redis command:

```
HSET person:3 name "Summer Smith" age 17
```

A registered function is by definition nonblocking, so any results it returns can only be obtained with the `RG.GETRESULTS` command and by specifying its respective execution ID. At this point we know there is just one registered function and that it had executed just once, so we can use `RG.DUMPEXECUTIONS` output to get the last execution's ID and then read the results:

!!! example "Example: dumping executions"
    ```
    127.0.0.1:6379> HSET person:3 name "Summer Smith" age 17
    (integer) 2
    127.0.0.1:6379> RG.DUMPEXECUTIONS
    1) 1) "executionId"
       2) "0000000000000000000000000000000000000000-1"
       3) "status"
       4) "done"
    2) 1) "executionId"
       2) "0000000000000000000000000000000000000000-0"
       3) "status"
       4) "done"
    127.0.0.1:6379> RG.GETRESULTS 0000000000000000000000000000000000000000-1
    1) 1) (integer) 17
    2) (empty array)
    ```

Note that the executions' list now consists of two entries: the first is the most recent one generated by the registered function, and the second entry is from our previous nonblocking execution of the batch function.

!!! note "Order of reply"
    The order of elements in the reply may be different than that of their creation.

The result "17" is technically correct, in the sense that it is the maximum of inputs and in this case, a single record. To have an event-driven maximum, we'll store its current value in the database.

## Writing Data
The RedisGears Python API ships with the [`execute()` function](runtime.md#execute), which allows the execution of arbitrary Redis commands in the database. RedisGears functions can call `execute()` for accessing the data during their flow, both for reading and writing, allowing the enrichment of inputs and persistence of results.

We'll complete the implementation that seeks an event-driven maximum by storing the current maximum value in another Redis key called `age:maximum`:

```python
{{ include('intro/intro-008.py') }}
```

The event handler employs a new step type after mapping the input records to ages. The [`foreach()`](operations.md#foreach) step executes its argument function callback once for each input record but does not change the records themselves. We use it to call the check-and-set logic that's implemented by `cas()` function.

!!! example "Example: Event-driven maximum"
    ```
    127.0.0.1:6379> GET age:maximum
    (nil)
    127.0.0.1:6379> HSET person:4 name "Beth Smith" age 35
    (integer) 2
    127.0.0.1:6379> GET age:maximum
    "35"
    127.0.0.1:6379> HSET person:5 name "Shrimply Pibbles" age 87
    (integer) 2
    127.0.0.1:6379> GET age:maximum
    "87"
    ```

??? note "Disclaimer"
    In reality, Shrimply Pibbles' age is unknown, so the above is only an estimate and may be inaccurate. Luckily, he no longer requires a heart transplant.

## Cluster 101
Redis can be used in one of two modes: **Stand-alone** or [**Cluster**](glossary.md#cluster).

When deployed in cluster mode, multiple Redis server processes that are referred to as [**Shards**](glossary.md#shard), manage a single logical database in a _shared-nothing_ fashion.

The database is partitioned by hashing the names of keys into slots, and each shard manages only the keys in the slots that it is assigned with. Every slot (and therefore every key) has a single shard managing it and that shard is called the **master**.

Masters can have zero or more **replica** shards, that are kept in sync for availability and read scaling purposes.

??? note "Further reference"
    To learn more about the cluster refer to the [Redis cluster tutorial](https://redis.io/topics/cluster-tutorial).

To quickly get a RedisGears-bootstrapped cluster consisting of 3 master shards you can use Docker:

```
docker run -d --name rgcluster -p 30001:30001 -p 30002:30002 -p 30003:30003 redislabs/rgcluster:latest
```

To load the test data to the cluster, first create a file called "data.txt" with these contents:

!!! summary "data.txt"
    ```
    SET foo bar
    HSET person:1 name "Rick Sanchez" age 70
    HSET person:2 name "Morty Smith" age 14
    HSET person:3 name "Summer Smith" age 17
    HSET person:4 name "Beth Smith" age 35
    HSET person:5 name "Shrimply Pibbles" age 87
    ```

Now, run the following command:

```
docker exec -i rgcluster redis-cli -c -p 30001 < data.txt
```

!!! important "Use `redis-cli -c` for cluster mode"
    The cli, by default, does not follow the cluster's redirections. To have the cli automagically hop between shards, start it with the `-c` command line switch.

The output should resemble the following:

!!! example "Example: populating the cluster with data"
    ```
    $ docker exec -i rgcluster redis-cli -c -p 30001 < data.txt
    -> Redirected to slot [12182] located at 127.0.0.1:30003
    OK
    -> Redirected to slot [1603] located at 127.0.0.1:30001
    (integer) 2
    -> Redirected to slot [13856] located at 127.0.0.1:30003
    (integer) 2
    -> Redirected to slot [9729] located at 127.0.0.1:30002
    (integer) 2
    (integer) 2
    -> Redirected to slot [1735] located at 127.0.0.1:30001
    (integer) 2
    ```

In more graphic terms, this illustrates the distribution of our data in the cluster:

```
 +----------------------+   +----------------------+   +----------------------+
 | Shard A:30001        |   | Shard B:30002        |   | Shard C:30003        |
 | +----------+-------+ |   | +----------+-------+ |   | +----------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | person:1 | {...} | |   | | person:3 | {...} | |   | | foo      | bar   | |
 | | person:5 | {...} | |   | | person:4 | {...} | |   | | person:2 | {...} | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 +----------------------+   +----------------------+   +----------------------+
```
## Distributed Processing
When RedisGears is running in a cluster, it will execute functions on all of the cluster's shards by default. That means that when a client sends an `RG.PYEXECUTE` command to one of the shards, for example shard A, that shard as well as all other shards in the cluster (shards B and C in our example) will execute that function in parallel.

To see this in action, we can run the simplest function from one of the shards and have all keys in the database returned:

!!! example "Example: distributed processing"
    ```
    $ redis-cli -c -p 30001
    127.0.0.1:30001> RG.PYEXECUTE "GB().run()"
    1) 1) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
    2) "{'key': 'person:5', 'value': {'age': '87', 'name': 'Shrimply Pibbles'}}"
    3) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
    4) "{'key': 'person:4', 'value': {'age': '35', 'name': 'Beth Smith'}}"
    5) "{'key': 'person:3', 'value': {'age': '17', 'name': 'Summer Smith'}}"
    6) "{'key': 'foo', 'value': 'bar'}"
    2) (empty list or set)
    ```

Although data is distributed across the cluster's shards, the function returns results that are identical (order excluded) to what a single-instance would have returned. This is because the originating shard had collected the intermediate local results from all other shards before returning a merged response.

An illustration may help in explaining what had happened:

```
 +----------------------+
 | Shard A              |
 | +------------------+ |
 | | Redis command    | | +------------+--------------------------+
 | | RG.PYEXECUTE ... | | |            |         Function         |
 | +-+----------------+ | | +----------|-----------+   +----------|-----------+
 |   v                  | | | Shard B  v           |   | Shard C  v           |
 | +-+----------------+ | | | +--------+---------+ |   | +--------+---------+ |
 | | Coordinator      +---+ | | Coordinator      | |   | | Coordinator      | |
 | | Send execution   | |   | | Send execution   | |   | | Send execution   | |
 | +-+----------------+ |   | +-+----------------+ |   | +-+----------------+ |
 |   v Function         |   |   v Function         |   |   v Function         |
 | +-+----------------+ |   | +-+----------------+ |   | +-+----------------+ |
 | | Engine           | |   | | Engine           | |   | | Engine           | |
 | | Local execution  | |   | | Local execution  | |   | | Local execution  | |
 | +-+----------------+ |   | +-+----------------+ |   | +-+----------------+ |
 |   v Local records    |   |   v Local records    |   |   v Local records    |
 | +-+----------------+ |   | +-+----------------+ |   | +-+----------------+ |
 | | Coordinator      +<--+ | | Coordinator      | |   | | Coordinator      | |
 | | Collect results  | | | | | Return results   | |   | | Return results   | |
 | +--------+---------+ | | | +--------+---------+ |   | +--------+---------+ |
 |          v           | | +----------|-----------+   +----------|-----------+
 |    Global results    | |            |       Local records      |
 +----------------------+ +------------+--------------------------+
```

Before returning the results, the coordinator of the originating shard collects the local results from each shard. This is the default behavior and using it implicitly adds a [`collect()`](operations.md#collect) operation to the function as its last step.

This can be disabled by providing the `collect=False` argument to the `run()` action. When no collection is performed, the results will consist only of the shard's local records:

!!! example "Example: `run(collect=False)`"
    ```
    127.0.0.1:30001> RG.PYEXECUTE "GB().run(collect=False)"
    1) 1) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       2) "{'key': 'person:5', 'value': {'age': '87', 'name': 'Shrimply Pibbles'}}"
    2) (empty list or set)
    ```

The `collect()` operation can be called at any point of the flow, so by adding it to this function we'll get results from all shards again despite having disabled the default one:

!!! example "Example: `collect()`"
    ```
    127.0.0.1:30001> RG.PYEXECUTE "GB().collect().run(collect=False)"
    1) 1) "{'key': 'person:1', 'value': {'age': '70', 'name': 'Rick Sanchez'}}"
       2) "{'key': 'person:5', 'value': {'age': '87', 'name': 'Shrimply Pibbles'}}"
       3) "{'key': 'person:2', 'value': {'age': '14', 'name': 'Morty Smith'}}"
       4) "{'key': 'foo', 'value': 'bar'}"
       5) "{'key': 'person:4', 'value': {'age': '35', 'name': 'Beth Smith'}}"
       6) "{'key': 'person:3', 'value': {'age': '17', 'name': 'Summer Smith'}}"
    2) (empty list or set)
    ```

## MapReduce
The RedisGears framework supports functions that follow the MapReduce pattern

??? quote "[Wikipedia: MapReduce](https://en.wikipedia.org/wiki/MapReduce)"
    A MapReduce framework (or system) is usually composed of three operations (or steps):

      1. **Map**: each worker node applies the map function to the local data, and writes the output to a temporary storage. A master node ensures that only one copy of the redundant input data is processed.
      2. **Shuffle**: worker nodes redistribute data based on the output keys (produced by the map function), such that all data belonging to one key is located on the same worker node.
      3. **Reduce**: worker nodes now process each group of output data, per key, in parallel.

In our example, data is localized by the cluster's partitioning to each master shard, and neither mapping or reducing is done on the **KeyReader**'s records. Shuffling occurs when `collect()` is called, moving all local records to the originating worker.

## Cluster Map and Reduce
To map and reduce the cluster's data, we can run the maximum function on the cluster. However, if we execute the function unchanged it will return the non-reduced results:

```python
{{ include('intro/intro-009.py') }}
```

The `accumulate()` operation is performed locally, on each master shard in parallel. The implicit `collect()` operation before the `run()` action (recall that `collect=True` by default) collects the shards' maxima, and these are returned as result.

Providing the correct result requires selecting the maximum of the maxima. To rectify this, we'll explicitly collect the local results, and apply an accumulation step to reduce them. This looks like this:

```python
{{ include('intro/intro-010.py') }}
```

There's another, shorter and much neater way to achieve the same. The RedisGears Python API includes the [`aggregate()`](operations.md#aggregate) operation that wraps the accumulate-collect-accumulate steps into a single one:

```python
{{ include('intro/intro-011.py') }}
```

`aggregate()` accepts three arguments: the first is the accumulator's zero value, and the other two are callbacks to accumulating functions that will be executed locally and globally, respectively. In the maximum's example above, the zero value is the scalar value zero, and both local and global are the same maximum-returning lambda function.

We can also use `aggregate()` for computing a reduced average:

```python
{{ include('intro/intro-012.py') }}
```

This time, we've provided a tuple of zeros as the zero value. The local function performs the equivalent of the previously-introduced `prepare_avg()`, and provides the sum and count of ages per worker. Then, once collected, the global callback merges the local tuple records by summing them. In the last `map()` step, much like with the `compute_avg()` function, the final value is computed.

??? tip "Advanced: look at `avg()`'s implementation"
    The RedisGears Python API `avg()` operation is implemented by the code in [GearsBuilder.py][https://github.com/RedisGears/RedisGears/blob/0.9/src/GearsBuilder.py#L93-L103] - you're encouraged to review and compare it to the above.

## Local vs. Global
Input records are determined by the worker's data and the function's reader type. While executing a distributed operation, records may need to be shuffled - a.k.a repartitioned - and moved to other workers.

!!! important "Performance matters"
    Repartitioning impacts performance, so try avoiding it as much as possible.

We've used the `collect()` operation to move all records to the originating worker, which is one repartitioning strategy. Another strategy is to distribute the records among workers by some chosen key, and as usual, we'll use examples to cover the details.

We'll set our final task to be counting the number of persons per family in our database. The quickest way to get this done is probably:

```python
{{ include('intro/intro-013.py') }}
```

Do not let the apparent simplicity of the above fool you - a lot of work done by the engine (and some Pythonic wrappers) make it happen. It should be pretty obvious what's happening here though: the [`countby()`](operations.md#countby) operation returns a count for each key in its input records. The function callback argument that it accepts is an extractor for the key, so in this case `fname()` returns the person's last name.

In reality, the `countby()` operation is implemented efficiently by an assortment of other steps. This is what it would look like if coded from scratch:

```python
{{ include('intro/intro-014.py') }}
```

We've introduced two new operations: [`localgroupby()`](operations.md#localgroupby) and [`groupby()`](operations.md#groupby). Both perform the same type of operation, that is the grouping of records but differ in regards of where they run.

The first operation, `localgroupby()`, is run locally by each shard's engine. The global `groupby()` applies the extractor function to the local data , shuffles the records to appropriate shards and then applies the accumulator function locally.


Both global and local group operations expect two functions as their arguments. The first function is an extractor, whereas the second is an accumulator. While the accumulating functions for previous operations we've used had only used two arguments (namely the accumulator, "a", and the record, "r"), group operations precede these with another "k" argument that represents the key on which grouping is performed.

The local grouping accumulator increases the count for each input family name record, whereas the global one sums them. Here's how the data moves:

```
 +----------------------+   +----------------------+   +----------------------+
 | Shard A              |   | Shard B              |   | Shard C              |
 | +----------+-------+ |   | +----------+-------+ |   | +----------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | person:1 | {...} | |   | | person:3 | {...} | |   | | foo      | bar   | |
 | | person:5 | {...} | |   | | person:4 | {...} | |   | | person:2 | {...} | |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 |   v localgroupby()   |   |   v localgroupby()   |   |   v localgroupby()   |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Sanchez  | 1     | |   | | Smith    | 2     | |   | | Smith    | 1     | |
 | | Pibbles  | 1     | |   | +-+----------------+ |   | +-+----------------+ |
 | +-+--------+-------+ |   |                      |   |                      |
 +----------------------+   +----------------------+   +----------------------+
 |   v repartition()    |   |   v repartition()    |   |   v repartition()    |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Pibbles  | 1     | |   | | Sanchez  | 1     | |   | | Smith    | 2     | |
 | +------------------+ |   | +------------------+ |   | | Smith    | 1     | |
 |                      |   |                      |   | +------------------+ |
 |   v localgroupby()   |   |   v localgroupby()   |   |   v localgroupby()   |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Pibbles  | 1     | |   | | Sanchez  | 1     | |   | | Smith    | 3     | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 |                      |   +---|------------------+   +---|------------------+
 |                      |       |                          |
 | +-+--------+-------+ |       |   Implicit collect()     |
 | | Key      | Value |<--------+--------------------------+
 | +------------------+ |
 | | Sanchez  | 1     | |
 | | Pibbles  | 1     | |
 | | Smith    | 3     | |
 | +------------------+ |
 +----------------------+
```

That's an efficient processing pattern because data is first reduced locally, which results in fewer records that need to be repartitioned by the `collect()`ion. Consider this less-than-recommended implementation for comparison:

```python
{{ include('intro/intro-015.py') }}
```

Using only our limited dataset it is unlikely that we'll be able to discern any difference in performance. Instead of collecting two records we'll be collecting three and that's hardly significant.

Consider, however, what will happen once we start adding Revolio Clockberg Jr. (a.k.a Gearhead), Mr. Poopybutthole, Birdperson, Fart and the rest of the multiverse to the database. The number of input records and the number of families will increase accordingly, causing more and more records to be moved across the network and resulting in higher latencies.

## Repartitioning Data
!!! important "Important reminder"
    Repartitioning impacts performance, so try avoiding it as much as possible.

As mentioned earlier, when absolutely required, functions can repartition data in the cluster by using an arbitrary key. When data is repartitioned, each worker is assigned with a subset of the records' keys and these are shipped to it from all other workers.

Let's make up a contrived example to demonstrate the inner workings. We'll add a requirement for storing  the families' head counts as simple strings in their respective String keys. Put differently, we expect that after running the function we'll be able to do this:

!!! example "Example: expect results of retrieving family head counts"
    ````
    127.0.0.1:30001> GET Smith
    -> Redirected to slot [14205] located at 127.0.0.1:30003
    "3"
    127.0.0.1:30003> GET Sanchez
    -> Redirected to slot [9503] located at 127.0.0.1:30002
    "1"
    127.0.0.1:30002> GET Pibbles
    -> Redirected to slot [169] located at 127.0.0.1:30001
    "1"
    ````

The trick in this case is ensuring that the target String keys we'll be using reside on the same shards as the workers that are writing them. The distribution of results should follow the cluster's partitioning scheme, just like that of the input records.

To do that, we'll modify the function to include the [`repartition()`](operations.md#repartition) operation:

```python
{{ include('intro/intro-016.py') }}
```

Here's how this function differs: instead of performing the global grouping operation, we've called `repartition()` in order to have the locally-grouped records shuffled in the cluster. By using the records' key, all records with the same key arrive to the same worker, allowing it to reduce them further with the summer.

!!! tip "Use `aggregateby()`"
    RedisGears' Python API includes the [`aggregateby()`](operations.md#aggregateby) operation. It amounts to the same as using the `GB().localgroupby().repartition().localgroupby()` flow.

Then, after shuffling and summing, each worker executes the `foreach()` operation on its family records for setting their respective keys and values in Redis. Put differently:

```
 +----------------------+   +----------------------+   +----------------------+
 | Shard A              |   | Shard B              |   | Shard C              |
 | +----------+-------+ |   | +----------+-------+ |   | +----------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | person:1 | {...} | |   | | person:3 | {...} | |   | | foo      | bar   | |
 | | person:5 | {...} | |   | | person:4 | {...} | |   | | person:2 | {...} | |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 |   v localgroupby()   |   |   v localgroupby()   |   |   v localgroupby()   |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Sanchez  | 1     | |   | | Smith    | 2     | |   | | Smith    | 1     | |
 | | Pibbles  | 1     | |   | |          |       | |   | |          |       | |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 |   v repartition()    |   |   v repartition()    |   |   v repartition()    |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value +<----->+ Key      | Value +<----->+ Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Pibbles  | 1     | |   | | Sanchez  | 1     | |   | | Smith    | 1     | |
 | |          |       | |   | |          |       | |   | | Smith    | 2     | |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 |   v localgroupby()   |   |   v localgroupby()   |   |   v localgroupby()   |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 | | Key      | Value | |   | | Key      | Value | |   | | Key      | Value | |
 | +------------------+ |   | +------------------+ |   | +------------------+ |
 | | Pibbles  | 1     | |   | | Sanchez  | 1     | |   | | Smith    | 3     | |
 | +-+--------+-------+ |   | +-+--------+-------+ |   | +-+--------+-------+ |
 |   v execute() ...    |   |   v execute() ...    |   |   v execute() ...    |
 +----------------------+   +----------------------+   +----------------------+
```

## Where Next?
At this point you should be pretty much acquainted with the basic principles under the hood of the RedisGears engine. To extend your familiarity and gain more experience with RedisGears it is recommend that you also review the following at your leisure:

  * The [Overview](glossary.md) page summarizes the concepts used by RedisGears
  * The reference pages about RedisGears' [Runtime](runtime.md), [Functions](functions.md), [Readers](readers.md) and [Operations](operations.md)
  * The RedisGears [Commands](commands.md) reference
  * The [Quickstart](quickstart.md) page provides information about getting, building, installing and running RedisGears
  * There are interesting uses and RedisGears recipes in the [Examples](examples.md)
