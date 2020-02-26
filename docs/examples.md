# RedisGears Examples
The following sections consist of various [recipes](overview.md#recipe) and basic examples showing the uses for RedisGears.

To contribute your own example or recipe (and get the credit for it), click the "Edit this page" button at the top to submit a Pull Request.

## Recipes
This is a list of RedisGears recipes that can be used as is or as an inspiration source.

| Recipe | Description | Author | License | URL |
| --- | --- | --- | --- | --- |
| WriteBehind | Write-Behind and Write-Through from Redis to other SQL/No-SQL databases | [RedisLabs](https://redislabs.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/WriteBehind/) |
| AnimalRecognitionDemo | An example of using Redis Streams, RedisGears and RedisAI for Realtime Video Analytics (i.e. filtering cats) | [RedisLabs](https://redislabs.com/) | BSD-3-Clause | [git](https://github.com/RedisGears/AnimalRecognitionDemo) |
| EdgeRealtimeVideoAnalytics | An example of using Redis Streams, RedisGears, RedisAI and RedisTimeSeries for Realtime Video Analytics (i.e. counting people) | [RedisLabs](https://redislabs.com/) | Apache-2.0 | [git](https://github.com/RedisGears/EdgeRealtimeVideoAnalytics) |

## Word Count

**Author: [RedisLabs](https://redislabs.com/)**

**Purpose**

The counting of words.

**Assumptions**

All keys store Redis String values. Each value is a sentence.

**Python API**

```python
# Create function's builder context
gb = GearsBuilder()

# Map the the records their "sentence" value
gb.map(lambda x: x['value'])

# Split sentence records to words
gb.flatmap(lambda x: x.split())

# Count each word's occurances
gb.countby()

# Batch run it
gb.run()
```

## Delete by Key Prefix

**Author: [RedisLabs](https://redislabs.com/)**

**Purpose**

Deletes all the keys with name beginning with a prefix and return their count.

**Assumptions**

There may be keys in the database. Some of these may have names beginning with the "delete_me:" prefix.

**Python API**

```python
# Create function's builder context
gb = GearsBuilder()

# Map the records to their key name
gb.map(lambda x: x['key'])

# Delete each key
gb.foreach(lambda x: execute('DEL', x))

# Count the records (deleted keys)
gb.count()

# Run as a batch on the 'delete_me:*' glob-like pattern
gb.run('delete_me:*')
```

## Basic Redis Stream Processing

**Author: [RedisLabs](https://redislabs.com/)**

**Purpose**

Copy every new message from the Redis Stream to a Redis Hash key.

**Assumptions**

An input Redis Stream is stored under the "mystream" key.

**Python API**

```python
# Create function's builder context
gb = GearsBuilder('StreamReader')

# Write the message to a Redis Hash
gb.foreach(lambda x: execute('HMSET', x['streamId'], *x))

# Register the function to events from 'mystream'
gb.register('mystream')
```

## Distributed Monte Carlo Estimation of Pi's Value

**Author: [RedisLabs](https://redislabs.com/)**

**Purpose**

Estimate value of Pi by throwing darts at a carefully-constructed dartboard.

!!! tip "There are far better ways to get Pi's value"
    This example is intended for educational purposes only. For all practical purposes you'd be better off using the constant value of 3.14159265359.

** Python API**

```python
TOTAL_DARTS = 1000000  # total number of darts

def inside(p):
    ''' Generates a random point that is or isn't inside the circle '''
    from random import random
    x, y = random(), random()
    return x*x + y*y < 1

def throws():
    ''' Calculates each shard's number of throws '''
    global TOTAL_DARTS
    throws = TOTAL_DARTS
    ci = execute('RG.INFOCLUSTER')
    if type(ci) is not str:                 # assume a cluster
        n = len(ci[2])                      # number of shards
        me = ci[1]                          # my shard ID
        ids = [x[1] for x in ci[2]].sort()  # shards' IDs list
        i = ids.index(me)                   # my index
        throws = TOTAL_DARTS // n           # minimum shard throws
        if i == 0 and TOTAL_DARTS % n > 0:  # first shard gets remainder
            throws += 1
    yield throws

def estimate(hits):
    ''' Estimates Pi's value from hits '''
    from math import log10
    hits = hits * 4                         # only quadrant of the board is used
    r = hits / 10 ** int(log10(hits))       # make it irrational
    return f'Pi\'s estimated value is {r}'

# Create function's builder context
gb = GB('PythonReader')

# Throw the local darts
gb.flatmap(lambda x: [i for i in range(int(x))])

# Throw out darts that had missed the board
gb.filter(inside)

# Count the remaining darts
gb.accumulate(lambda a, x: 1 + (a if a else 0))

# Collect the results
gb.collect()

# Merge darts' counts
gb.accumulate(lambda a, x: x + (a if a else 0))

# The are four pieces of pie in Pi
gb.map(estimate)

# Run it
gb.run(throws)
```
