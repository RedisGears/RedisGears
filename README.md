[![GitHub issues](https://img.shields.io/github/release/RedisGears/RedisGears.svg?sort=semver)](https://github.com/RedisGears/RedisGears/releases)
[![CircleCI](https://circleci.com/gh/RedisGears/RedisGears/tree/master.svg?style=svg)](https://circleci.com/gh/RedisGears/RedisGears/tree/master)
[![Dockerhub](https://img.shields.io/badge/dockerhub-redislabs%2Fredisgears-blue)](https://hub.docker.com/r/redislabs/redisgears/tags/)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/RedisGears/RedisGears.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RedisGears/RedisGears/alerts/)

# RedisGears
[![Forum](https://img.shields.io/badge/Forum-RedisGears-blue)](https://forum.redislabs.com/c/modules/redisgears)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/6yaVTtp)

<img src="docs/images/RedisGears.png" alt="logo" style="width: 2em; vertical-align: middle;"/> 

RedisGears is a dynamic framework for data processing in Redis. RedisGears supports transaction, [batch](docs/glossary.md#batch-processing), and [event-driven](docs/glossary.md#event-processing) processing of Redis data. To use RedisGears, you write [functions](docs/functions.md) that describe how your data should be processed. You then submit this code to your Redis deployment for remote execution.

As of v1.0.0, code for RedisGears must be written in Python. However, an internal C [API](glossary.md#api) exists and can be used by other Redis modules. Support for other languages is being planned.

* The RedisGears Homepage: https://oss.redislabs.com/redisgears
* Get to sixth gear with an [Introduction to RedisGears](https://oss.redislabs.com/redisgears/intro.html)
* Running, building, and installing are covered by the [Quickstart page](https://oss.redislabs.com/redisgears/quickstart.html)
* Check out some of the [examples](https://oss.redislabs.com/redisgears/examples.html)

## Contact Us
If you have questions or feedback, or want to report an issue or [contribute some code](https://cla-assistant.io/RedisGears/RedisGears), here's where you can get in touch:

  * [Forum](https://forum.redislabs.com/c/modules/redisgears)
  * [Discord chat](https://discord.gg/6yaVTtp)
  * [Report an issue](https://github.com/RedisGears/RedisGears/issues)

## License
RedisGears is licensed under the [Redis Source Available License Agreement](LICENSE).
