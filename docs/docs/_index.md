---
title: Triggers and Functions
description: Trigger and execute JavaScript functions in the Redis process
linktitle: Triggers and Functions
type: docs
---

[![discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)
[![Github](https://img.shields.io/static/v1?label=&message=repository&color=5961FF&logo=github)](https://github.com/RedisGears/RedisGears/)

# Triggers and Functions is currently in Preview

Triggers and Functions allows running JavaScript functions inside the Redis Process. These functions can be executed on-demand, by an event-driven trigger or by a stream processing trigger.

## Quick links

* [Quick start guide](/docs/stack/redisgears/quick_start)
* [Source code](https://github.com/RedisGears/RedisGears)
* [Latest release](https://github.com/RedisGears/RedisGears/releases)
* [Docker image](https://hub.docker.com/r/redis/redis-stack-server/)

## Primary features

* JavaScript engine for functions
* On-demand functions
* Keyspace triggers
* Stream triggers
* Async handling of functions
* Read data from across the cluster

## Cluster support

The triggers and functions feature of Redis Stack support deployment and execution of functions across a cluster. Functions are executed on the correct shard based on the key that is changed or read functions can be executed on all to return a correct view of the data.

## References

### Blog posts

- [Expanding the Database Trigger Features in Redis](https://redis.com/blog/database-trigger-features/)

## Overview
