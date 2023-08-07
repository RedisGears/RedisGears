---
title: Triggers and functions
description: Trigger and execute JavaScript functions in the Redis process
linktitle: Triggers and functions
type: docs
stack: true
bannerText: The triggers and functions feature of Redis Stack and its documentation are currently in preview, and only available in Redis Stack 7.2 RC3 or later. If you notice any errors, feel free to submit an issue to GitHub using the "Create new issue" link in the top right-hand corner of this page.
bannerChildren: true
---

[![discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)
[![Github](https://img.shields.io/static/v1?label=&message=repository&color=5961FF&logo=github)](https://github.com/RedisGears/RedisGears/)

# Triggers and functions

The triggers and functions feature of Redis Stack provides for running JavaScript functions inside the Redis Process. These functions can be executed on-demand, by an event-driven trigger, or by a stream processing trigger.

## Quick links

* [Quick start guide](/docs/interact/programmability/triggers-and-functions/quick_start)
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

Triggers and functions support deployment and execution of functions across a cluster. Functions are executed on the correct shard based on the key that is changed or read functions can be executed on all to return a correct view of the data.

## References

### Blog posts

- [Expanding the Database Trigger Features in Redis](https://redis.com/blog/database-trigger-features/)

## Overview
