# RedisGears Glossary

## Redis

**Redis** is an ...

!!! quote
    ... open source (BSD licensed), in-memory data structure store, used as a database, cache and message broker. It supports data structures such as strings, hashes, lists, sets, sorted sets with range queries, bitmaps, hyperloglogs, geospatial indexes with radius queries, and streams. Redis has built-in replication, Lua scripting, LRU eviction, transactions, and different levels of on-disk persistence, and provides high availability via Redis Sentinel and automatic partitioning with Redis Cluster.

!!! tip "Redis is also..."
    An acronym for **RE**mote **DI**ctionary **S**erver that's pronounced "red" like the colour, then "iss".

??? info "Further reference"
    More about Redis:

      * [Home page](https://redis.io)
      * [Interactive tutorial](http://try.redis.io)
      * [Repository](https://github.com/antirez/redis)


## Module
A Redis **module** is a shared library that can be loaded by the Redis server during runtime. Redis modules extend Redis with new data types and commands.

??? info "Further reference"
    You can learn more about Redis modules at:

      * [Redis Modules API](https://redis.io/documentation#redis-modules-api)
      * [Modules published at redis.io](https://redis.io/modules)

## Client
A Redis **client** is a piece of software you use to interact with Redis. All Redis clients speak the [Redis Protocol](https://redis.io/topics/protocol).

??? info "Further reference"
    For a complete list of clients, refer to:

      * [Redis clients by programming language](https://redis.io/clients)
      * [redis-cli](https://redis.io/topics/rediscli)

## Command
An instruction sent to the Redis server by a client. Both Redis and Redis modules implement commands.

??? info "Further reference"
    Refer to these pages for more information about:

      * [Redis commands](https://redis.io/commands)
      * [RedisGears commands](commands.md)

## Data
Zeros and ones that make up for everything. Usually stored in Redis, but can also be obtained from other sources.

## Data Flow
A logical operation that processes input data in some form and may consist of several steps.

## Data Type
Redis stores data in structures that implement different types and the API to manipulate their contents. Types can be added by modules, and the core Redis server provides support for Strings, Hashes, Lists, Sets, Sorted Sets and Streams.

??? info "Further reference"
    Refer to these pages for more information about Redis' core types:

      * [Redis data types and abstractions](https://redis.io/topics/data-types-intro)
      * [Redis Streams](https://redis.io/topics/streams-intro)

## Event
An **event** is a signal that the RedisGears engine can intercept and react to by triggering the execution of an event-driven processing function.

## Engine
A RedisGears component in charge of the execution of functions. The engine executes locally on each of the cluster's shards.

## Coordinator
A RedisGears component that handles cluster communication and data shuffling.

## Cluster
A **cluster** is a deployment option for Redis in which the database is partitioned across multiple shards in a _shared-nothing_ fashion. Partitioning is done by hashing key names to slots.

??? info "Further reference"
    More about the Redis cluster:

      * [Redis cluster tutorial](https://redis.io/topics/cluster-tutorial)

## Node
A **node** is a physical (or virtualized) server in a cluster. Each node can be used for hosting one or more shards.

## Shard
A **shard** is a Redis server process that runs on one of the cluster's nodes and manages an exclusive subset of slots in the Redis keyspace. Every slot is managed by a single master shard. Master shards accepts writes and can optionally be configured with replicas for scaling read throughput and increasing availability.

## API
RedisGears functions can be programmed via applicative interfaces. RedisGears provides a public Python API. RedisGears also provides a C API, which can be used internally by other Redis modules.

## Function
A RedisGears **function** is a an implementation of the processing steps in a data flow.

A function always:

  1. Starts with a reader
  2. Operates on zero or more records
  3. Consists of zero or more operations (steps)
  4. Ends with an action
  5. Returns zero or more results

??? info "Further reference"
    Refer to the [Functions page](functions.md) for a complete reference about functions.

## Reader
A RedisGears **reader** is the mandatory first step of any function. Every function has exactly one reader. A reader reads data and generates input records from it. The input records are consumed by the function.

??? info "Further reference"
    Refer to the [Readers page](readers.md) for a complete reference about the various reader types and their operation.

## Record
A RedisGears **record** is the basic abstraction that represents data in the function's flow. Input data records are passed from one step to the next and are finally returned as the result.

## Operation
An **operation** is the building block of RedisGears functions. Different operation types can be used to achieve a variety of results to meet various data processing needs.

??? info "Further reference"
    Refer to the [Operations page](operations.md) for a complete reference about the different operation types and their purpose.

## Step
A RedisGears function flow **step** is a term that's almost always interchangeable with "operation".

## Action
An **action** is special type of operation that is always the function's final step. There are two types of actions:

  1. **Run**: runs the function immediately in batch
  2. **Register**: registers the function's execution to be triggered by an event

??? info "Further reference"
    Actions are described at the [Functions page](functions.md#action).

## Result
The **result** of a function is the set of output records it produces after executing.

??? info "Further reference"
    Results are described at the [Functions page](functions.md#result).

## Execution
An **execution** is a single run of a RedisGears function. Every execution is uniquely identified by an ID, and every execution consists of the results, errors, and other information generated while executing.

??? info "Further reference"
    Executions are described at the [Functions page](functions.md#execution).

## Registration
A RedisGears **registration** is a stored function that is triggered by events.

??? info "Further reference"
    Registrations are described at the [Functions page](functions.md#registration).

## Session
A RedisGears **session** is an entity that is shared between all the executions and registrations created on the same [RG.PYEXECUTE](commands.md#rgpyexecute) command. A session maintains everything that it created and dropped when **all** the registrations and execution that belongs to has dropped. It is possible to see information about sessions using [RG.PYDUMPSESSIONS](commands.md#rgpydumpsessions) command.

## Initiator
In the context of a clustered environment, the **initiator** is the shard's engine in which a function is originally executed.

## Worker
In the context of a clustered environment, a **worker** is any shard that isn't the initiator.

## Batch Processing
A RedisGears function is executed as a batch when it is [run](functions.md#run) immediately on existing data.

Batch execution is always asynchronous. By default, the client executing the function is blocked until execution is finished. Once finished, RedisGears unblocks the client and returns any results or errors.

## Event Processing
A RedisGears function is executed as an event when it is [registered](functions.md#register) to be triggered by events.

## MapReduce
A framework for performing distributed data processing.

## Recipe
A RedisGears **recipe** is a collection of functions and their dependencies. A recipe typically implements a complex procedure, such as write-behind. You can think of a recipe as a "project".
