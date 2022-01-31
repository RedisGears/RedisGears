# RedisGears Examples
The following sections consist of various [recipes](glossary.md#recipe) and basic examples showing the uses for RedisGears.

To contribute your example or recipe (and get the credit for it), click the "Edit this page" button at the top to submit a Pull Request.

## Recipes
This is a list of RedisGears recipes that can be used as-is or as a source of inspiration.

| Recipe | Description | Author | License | URL |
| --- | --- | --- | --- | --- |
| WriteBehind | Write-Behind and Write-Through from Redis to other SQL/No-SQL databases | [Redis](https://redis.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/WriteBehind/) |
| AnimalRecognitionDemo | An example of using Redis Streams, RedisGears and RedisAI for Real-time Video Analytics (i.e. filtering cats) | [Redis](https://redis.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/AnimalRecognitionDemo) |
| EdgeRealtimeVideoAnalytics | An example of using Redis Streams, RedisGears, RedisAI and RedisTimeSeries for Realtime Video Analytics (i.e. counting people) | [Redis](https://redis.com/) | Apache-2.0 | [git](https://github.com/RedisGears/EdgeRealtimeVideoAnalytics) |
| FraudDetectionDemo | An example that combines several Redis data structures and along with RedisGears and RedisAI to showcase the advantage of data locality during transaction scoring | [Redis](https://redis.com) | BSD-3-Clause | [git](https://github.com/RedisAI/FraudDetectionDemo)|
| AdGears | Using RedisGears to maximize advertising revenue example.  Utilizes RedisGears, RedisTimeSeries and RedisBloom.| [Redis](https://redis.com) | BSD-3-Clause | [git](https://github.com/Redislabs-Solution-Architects/AdGears)|

## Word Count
The counting of words.

**Author: [Redis](https://redis.com/)**

**Assumptions**

All keys store Redis String values. Each value is a sentence.

**Python API**

```python
{{ include('examples/word-count.py') }}
```

## Delete by Key Prefix
Deletes all keys whose name begins with a specified prefix and return their count.

**Author: [Redis](https://redis.com/)**

**Assumptions**

There may be keys in the database. Some of these may have names beginning with the "delete_me:" prefix.

**Python API**

```python
{{ include('examples/del-by-prefix.py') }}
```

## Average on age field in json
Calculates the average `age` scanning all JSON docs keys that start with prefix `docs`

**Author: [Redis](https://redis.com/)**

**Assumptions**

[JSON module](https://oss.redis.com/redisjson/) is also loaded to Redis

**Python API**

```python
{{ include('examples/json-avg.py') }}
```

## Basic Redis Stream Processing

Copy every new message from a Redis Stream to a Redis Hash key.

**Author: [Redis](https://redis.com/)**

**Assumptions**

An input Redis Stream is stored under the "mystream" key.

**Python API**

```python
{{ include('examples/stream-logger.py') }}
```

## Automatic Expiry

Sets the time to live (TTL) for every updated key to one hour.

**Author: [Redis](https://redis.com/)**

**Python API**

```python
{{ include('examples/automatic-expire.py') }}
```

**Author: [Redis](https://redis.com/)**

## Keyspace Notification Processing

This example demonstrates a two-step process that:

1. Synchronously captures distributed keyspace events
1. Asynchronously processes the events' stream

Specifically, the example shows how expired key names can be output to the log.

**Author: [Redis](https://redis.com/)**

```python
{{ include('examples/notification-processing.py') }}
```

## Reliable Keyspace Notification

Capture each keyspace event and store to a Stream

**Author: [Redis](https://redis.com/)**

**Python API**

```python
{{ include('examples/reliable-keyspace-notification.py') }}
```

## Distributed Monte Carlo to Estimate _pi_

Estimate _pi_ by throwing darts at a carefully-constructed dartboard.

!!! tip "There are far better ways to get Pi's value"
    This example is intended for educational purposes only. For all practical purposes, you'd be better off using the constant value 3.14159265359.

**Author: [Redis](https://redis.com/)**

**Python API**

```python
{{ include('examples/monte-carlo-pi.py') }}
```
