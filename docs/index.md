<center>![logo.png](./images/RedisGears.png)</center>

# RedisGears - Dynamic execution for Redis data

simply:
```
GearsBuilder().filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run('*')
```
RedisGears supports full python syntax and low level c api. In addition you can run it on cluster.

!!! note "Quick Links:"
    * [Source Code at GitHub](https://github.com/RedisLabsModules/RedisGears).
    * [Latest Release: 0.2.0](https://github.com/RedisLabsModules/RedisGears/releases)
    * [Docker Image: redislabs/redisgears](https://hub.docker.com/r/redislabs/redisgears/)
    * [Quick Start Guide](quickstart.md)
    * [Mailing list / Forum](https://groups.google.com/forum/#!forum/redisgears)

!!! tip "Supported Platforms"
    RedisGears is developed and tested on Linux x86_64 CPUs.

# Future plans
* GearsQL - SQL like language for quering your data.

# License

Redis Source Available License Agreement - see [LICENSE](https://github.com/RedisLabsModules/RedisGears/blob/master/LICENSE)

