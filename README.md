# RedisGears
Dynamic execution framework for your Redis data, simply:
```
gearsCtx('execution-name').filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run('*')
```
RedisGears supports full python syntax and low level c api. In addition you can run it on cluster.

# Quick Start
Install [redis 5.0](https://redis.io/) on you machine

Clone the git repository and run:
```
make get_deps
make WITHPYTHON=1
```

Notice that RedisGears dynamicly liked with your local python installation, it is possible to run without python support by just typing `make`.

run: `redis-server --loadmodule ./redisgears.so`

Add some keys to your redis server and then run:
```
rg.pyexecute "gearsCtx('test').map(lambda x:str(x)).run('*')"
rg.getresultsblocking test
```
You will get all of your keys and values in redis.

# RedisGears components:
## Execution Plan:
RedisGears end goal is to allow the user building Execution Plans and run them. Each execution plan starts with Reader, which supply the data, follow by a chain of operations and ends with run or register (will be explained in details later).

## Record
Record is the most basic object in RedisGears. A Record go through the entire execution plan and in the end returned to the user as a result.

## Reader
The reader is the first component on each execution, The reader responsable for suppling Record to the other components in the execution plan. RedisGears comes with a default reader that reads keys from redis. Using python it is possible to use the default reader with the flowing syntax: `gearsCtx(<execution name>)`, this line will return a gears context (which is basicly saves the exectution plan) with the default reader that reads the keys with the given prefix. The default reader will return Records in the following format:
```
{'key':< key as string >, 'value': < value (value type is depend on the key content) >}
```

## Operation
Operation is a logic unit that can be applied on the Record. Each operation recieves the Record as input and return result according to the operation type. The supported operation types are:

### Map
Map operation receive a Record and return another Record, example (using python api):
```
ctx.map(lambda r : str(r)) # transform a Record into a string Record
```

### FlatMap
Just like map but if the result is a list it flatten it right after, example (using python api):
```
ctx.flatMap(lambda r : [r, r]) # pass each record twice in the execution plan
```

### Limit
Limit the number of return results, limit operation gets the offset and the len and return result start from offset and up to len. Notice, limit is a single shard operation, if you want to limit the total number of the execution results you need to use limit then collect and then limit again. example (using python api):
```
ctx.limit(1, 3) # will return results 1 and 2.
```

### Filter
Filter operation receive a Record and return a boolean indicating whether or not the record should continue with the execution, example (using python api)
```
ctx.filter(lambda r : int(r['value']) > 50) # continue with the record if its value is greater then 50
```

### Groupby
Groupby operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key and list of records that grouped together by this key, the reducer return a new Record which is the reduce operation on all the Record in the same group. example (using python api)
```
ctx.groupby(lambda r : r[value], lambda key,vals: len(vals)) # count how many times each value appeared
```

### Collect
Return the results to the initiator (this operation has meaning only on cluster with more then one node, otherwise it has no meaning and it actually do nothing). example (using python api):
```
ctx.collect()
```

### Repartition
Repartition the record between the cluster nodes (this operation has meaning only on cluster with more then one node, otherwise it has no meaning and it actually do nothing). This operation receives an extractor, the repartition is perfomed by calculating the hslot on the extracted data and then move the record to the node hold this hslot.
example (using python api):
```
gearsCtx('test').repartition(lambda x: x['value']) # repartition record by value
```

### Write
Write is similar to map but it does not return any value, the write operation goal is to write results to the redis. After performing the write operation the execution plan will continue with the same records. Notice, on cluster, it is possible to write keys to shards that does not hold the keys hslot, its the user responsability to make sure this does not happened using repartition.
example (using python api):
```
gearsCtx('test').write(lambda x: redistar.execute_command('set', x['value'], x['key'])) # will save value as key and key as value
```

# Cluster Support
RedisGears support all of the operations on oss-cluster. Notice that the module needs to be loaded on all the cluster nodes. In addition, after setting up the cluster you need to run `rs.refreshcluster` on each node.

## Future plans
* Support more operations: SortBy, LocalGroupBy (for faster groupby on oss-cluster), Accumulate, ToList
* GearsQL - SQL like language for quering your data.

# License

Apache 2.0 with Commons Clause - see [LICENSE](LICENSE)

