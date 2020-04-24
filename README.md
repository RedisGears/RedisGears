[![GitHub issues](https://img.shields.io/github/release/RedisGears/RedisGears.svg?sort=semver)](https://github.com/RedisGears/RedisGears/releases)
[![CircleCI](https://circleci.com/gh/RedisGears/RedisGears/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/RedisGears/tree/master)
[![Docker Cloud Build Status](https://img.shields.io/docker/cloud/build/redislabs/redisgears.svg)](https://hub.docker.com/r/redislabs/redisgears/builds/)
[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Gitter](https://badges.gitter.im/RedisLabs/RedisGears.svg)](https://gitter.im/RedisLabs/RedisGears?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

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

## Mailing List / Forum
Got questions? Feel free to ask at the [RedisGears forum](https://forum.redislabs.com/c/modules/redisgears).

# License
Redis Source Available License Agreement - see [LICENSE](LICENSE)

