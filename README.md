[![CircleCI](https://circleci.com/gh/RedisLabsModules/RediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabsModules/RedisGears/tree/master)

# RedisGears
![logo.png](docs/images/RedisGears.png)

Dynamic execution framework for your Redis data, simply:
```
GearsBuilder().filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run('*')
```
RedisGears supports full python syntax and low level c api. In addition you can run it on cluster.

# Content

* [Quick Start](/docs/index.md)
* [Overview](/docs/overview.md)

# Future plans
* GearsQL - SQL like language for quering your data.

# License

Redis Source Available License Agreement - see [LICENSE](LICENSE)

