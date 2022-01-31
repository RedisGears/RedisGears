# <img src="images/RedisGears.png" alt="logo" style="width: 2em; vertical-align: middle;"/> RedisGears
[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/6yaVTtp)

## What is RedisGears?
RedisGears is an engine for data processing in Redis. RedisGears supports transaction, [batch](glossary.md#batch-processing), and [event-driven](glossary.md#event-processing) processing of Redis data. To use RedisGears, you write [functions](functions.md) that describe how your data should be processed. You then submit this code to your Redis deployment for remote execution.

As of v1.0.0, code for RedisGears must be written in Python. However, an internal C [API](glossary.md#api) exists and can be used by other Redis modules. In addition, support for other languages is being planned.

To take a simple example, you can use RedisGears to [count the words](examples.md#word-count) in Redis:

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
  * The [Overview](glossary.md) page summarizes important RedisGears concepts
  * The reference pages describe RedisGears' [Runtime](runtime.md), [Functions](functions.md), [Readers](readers.md), [Operations](operations.md) and integration with [RedisAI](redisai.md)
  * The RedisGears [Commands](commands.md) reference describes all commands
  * The [Quickstart](quickstart.md) page provides information about getting, building, installing, and running RedisGears
  * There are interesting RedisGears uses cases and recipes on the [Examples](examples.md) page

## Quick Links
  * [Source code repository](https://github.com/RedisGears/RedisGears)
  * [Releases](https://github.com/RedisGears/RedisGears/releases)
  * [Docker image](https://hub.docker.com/r/redislabs/redisgears/)

## Contact Us
If you have questions or feedback, or want to report an issue or [contribute some code](https://cla-assistant.io/RedisGears/RedisGears), here's where you can get in touch:

  * [Mailing list](https://forum.redislabs.com/c/modules/redisgears)
  * [Discord chat](https://discord.gg/6yaVTtp)
  * [Repository](https://github.com/RedisGears/RedisGears/issues)

## License
RedisGears is licensed under the [Redis Source Available License Agreement](https://github.com/RedisGears/RedisGears/blob/master/LICENSE).
