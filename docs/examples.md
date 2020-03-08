# RedisGears Examples
The following sections consist of various [recipes](overview.md#recipe) and basic examples showing the uses for RedisGears.

To contribute your example or recipe (and get the credit for it), click the "Edit this page" button at the top to submit a Pull Request.

## Recipes
This is a list of RedisGears recipes that can be used as-is or as an inspiration source.

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
gb = GearsBuilder()
gb.map(lambda x: x['value'])     # map records to "sentence" values
gb.flatmap(lambda x: x.split())  # split sentences to words
gb.countby()                     # count each word's occurances
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
gb = GearsBuilder()
gb.map(lambda x: x['key'])               # map the records to key names
gb.foreach(lambda x: execute('DEL', x))  # delete each key
gb.count()                               # count the records
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
gb = GearsBuilder('StreamReader')
gb.foreach(lambda x: execute('HMSET', x['streamId'], *x))  # write to Redis Hash
gb.register('mystream')
```

## Distributed Monte Carlo Estimation of Pi's Value

**Author: [RedisLabs](https://redislabs.com/)**

**Purpose**

Estimate Pi by throwing darts at a carefully-constructed dartboard.

!!! tip "There are far better ways to get Pi's value"
    This example is intended for educational purposes only. For all practical purposes, you'd be better off using the constant value of 3.14159265359.

** Python API**

```python
TOTAL_DARTS = 1000000                            # total number of darts

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
    if type(ci) is not str:                       # assume a cluster
        n = len(ci[2])                            # number of shards
        me = ci[1]                                # my shard's ID
        ids = [x[1] for x in ci[2]].sort()        # shards' IDs list
        i = ids.index(me)                         # my index
        throws = TOTAL_DARTS // n                 # minimum throws per shard
        if i == 0 and TOTAL_DARTS % n > 0:        # first shard gets remainder
            throws += 1
    yield throws

def estimate(hits):
    ''' Estimates Pi's value from hits '''
    from math import log10
    hits = hits * 4                               # one quadrant is used
    r = hits / 10 ** int(log10(hits))             # make it irrational
    return f'Pi\'s estimated value is {r}'

gb = GB('PythonReader')
gb.flatmap(lambda x: [i for i in range(int(x))])  # throw the local darts
gb.filter(inside)                                 # throw out missed darts
gb.accumulate(lambda a, x: 1 + (a if a else 0))   # count the remaining darts
gb.collect()                                      # collect the results
gb.accumulate(lambda a, x: x + (a if a else 0))   # merge darts' counts
gb.map(estimate)                                  # four pieces of pie
gb.run(throws)
```
