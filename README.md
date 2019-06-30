[![CircleCI](https://circleci.com/gh/RedisGears/RedisGears/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/RedisGears/tree/master)

# RedisGears
![logo.png](docs/images/RedisGears.png)

Dynamic execution framework for your Redis data, simply:
```
GearsBuilder().filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run('*')
```
RedisGears supports full python syntax and low level c api. In addition you can run it on cluster.

# Content

* [Quick Start](/docs/quickstart.md)
* [Overview](/docs/overview.md)
* [Commands](/docs/commands.md)
* [Streaming API](/docs/gears_streaming.md)
* [Examples](/docs/examples.md)
* [Gears Configuration](/docs/configuration.md)
* [Sub-interpreters](/docs/subinterpreters.md)

# Future plans
* GearsQL - SQL like language for quering your data.

# License

Redis Source Available License Agreement - see [LICENSE](LICENSE)

