# RedisGears Examples
The following sections consist of various [recipes](glossary.md#recipe) and basic examples showing the uses for RedisGears.

To contribute your example or recipe (and get the credit for it), click the "Edit this page" button at the top to submit a Pull Request.

## Recipes
This is a list of RedisGears recipes that can be used as-is or as an inspiration source.

| Recipe | Description | Author | License | URL |
| --- | --- | --- | --- | --- |
| WriteBehind | Write-Behind and Write-Through from Redis to other SQL/No-SQL databases | [RedisLabs](https://redislabs.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/WriteBehind/) |
| AnimalRecognitionDemo | An example of using Redis Streams, RedisGears and RedisAI for Realtime Video Analytics (i.e. filtering cats) | [RedisLabs](https://redislabs.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/AnimalRecognitionDemo) |
| EdgeRealtimeVideoAnalytics | An example of using Redis Streams, RedisGears, RedisAI and RedisTimeSeries for Realtime Video Analytics (i.e. counting people) | [RedisLabs](https://redislabs.com/) | Apache-2.0 | [git](https://github.com/RedisGears/EdgeRealtimeVideoAnalytics) |
| FraudDetectionDemo | An example that combines several Redis data structures and along with RedisGears and RedisAI to showcase the advantage of data locality during transaction scoring | [RedisLabs](https://redislabs.com) | BSD-3-Clause | [git](https://github.com/RedisAI/FraudDetectionDemo)|

## Word Count
The counting of words.

**Author: [RedisLabs](https://redislabs.com/)**

**Assumptions**

All keys store Redis String values. Each value is a sentence.

**Python API**

```python
{{ include('examples/word-count.py') }}
```

## Delete by Key Prefix
Deletes all the keys with name beginning with a prefix and return their count.

**Author: [RedisLabs](https://redislabs.com/)**

**Assumptions**

There may be keys in the database. Some of these may have names beginning with the "delete_me:" prefix.

**Python API**

```python
{{ include('examples/del-by-prefix.py') }}
```

## Basic Redis Stream Processing

Copy every new message from the Redis Stream to a Redis Hash key.

**Author: [RedisLabs](https://redislabs.com/)**

**Assumptions**

An input Redis Stream is stored under the "mystream" key.

**Python API**

```python
{{ include('examples/stream-logger.py') }}
```

## Automatic Expiry

Sets the time to live (TTL) for every updated key to one hour.

**Author: [RedisLabs](https://redislabs.com/)**

**Python API**

```python
{{ include('examples/automatic-expire.py') }}
```

**Author: [RedisLabs](https://redislabs.com/)**

## Keyspace Notification Processing

This example demonstrates a two-step process that:

1. Synchronously captures distributed keyspace events
1. Asynchronously processes the events' stream

Specifically, the example shows how expired key names can be output to the log.

**Author: [RedisLabs](https://redislabs.com/)**

```python
{{ include('examples/notification-processing.py') }}
```

## Distributed Monte Carlo Estimation of Pi's Value

Estimate Pi by throwing darts at a carefully-constructed dartboard.

!!! tip "There are far better ways to get Pi's value"
    This example is intended for educational purposes only. For all practical purposes, you'd be better off using the constant value of 3.14159265359.

**Author: [RedisLabs](https://redislabs.com/)**

**Python API**

```python
{{ include('examples/monte-carlo-pi.py') }}
```
