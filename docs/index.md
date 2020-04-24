# <img src="images/RedisGears.png" alt="logo" style="width: 2em; vertical-align: middle;"/> RedisGears
[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGears.svg)](https://gitter.im/RedisLabs/RedisGears?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## What is RedisGears?
RedisGears is a serverless engine for transaction, [batch](glossary.md#batch-processing) and [event-driven](glossary.md#event-processing) data processing in Redis. It is a dynamic framework for the execution of [functions](functions.md) that, in turn, implement data flows in Redis, while (almost) entirely abstracting the data's distribution and choice of deployment  (i.e. stand-alone vs. cluster, OSS vs. Enterprise). Functions can be implemented in different languages, including Python and C [APIs](glossary.md#api).

For example, you can use RedisGears to [count the words](examples.md#word-count) in Redis:

```python
{{ include('examples/word-count.py') }}
```

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

## Where Next?
  * The [Introduction](intro.md) is the recommended starting point
  * The [Overview](glossary.md) page summarizes the concepts used by RedisGears
  * The reference pages about RedisGears' [Runtime](runtime.md), [Functions](functions.md), [Readers](readers.md) and [Operations](operations.md)
  * The RedisGears [Commands](commands.md) reference
  * The [Quickstart](quickstart.md) page provides information about getting, building, installing and running RedisGears
  * There are interesting uses and RedisGears recipes in the [Examples](examples.md)

## Quick Links
  * [Source code repository](https://github.com/RedisGears/RedisGears)
  * [Releases](https://github.com/RedisGears/RedisGears/releases)
  * [Docker image](https://hub.docker.com/r/redislabs/redisgears/)

## Contact Us
If you have questions, want to provide feedback or perhaps report an issue or [contribute some code](contrib.md), here's where we're listening to you:

  * [Mailing list](https://forum.redislabs.com/c/modules/redisgears)
  * [Gitter chatroom](https://badges.gitter.im/RedisLabs/RedisGears.svg)
  * [Repository](https://github.com/RedisGears/RedisGears/issues)

## License
RedisGears is licensed under the [Redis Source Available License Agreement](https://github.com/RedisGears/RedisGears/blob/master/LICENSE).
