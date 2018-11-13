# RediStar
Allow performing chain of operations on redis datatypes, simply:
```
starCtx('execution-name', '*').filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run()
```
RediStar supports full python syntax and low level c api. In addition you can run it on oss cluster.

# Quick Start
Install [redis 5.0](https://redis.io/) on you machine

Clone the git repository and run:
```
make WITHPYTHON=1
```

Notice that RediStar dynamicly liked with your local python installation, it is possible to run without python support by just typing `make`.

run: `redis-server --loadmodule ./redistar.so`

Add some keys to your redis server and then run:
```
rs.execute "starCtx('test', '*').map(lambda x:str(x)).run()"
rs.getresultsblocking test
```
You will get all of your keys and values in redis.

# RediStar components:
## Execution Plan:
RediStar end goal is to allow the user building Execution Plans and run them. Each execution plan starts with Reader, which supply the data, follow by a chain of operations and ends with a Writer that returns the data to the user.

## Record
Record is the most basic object in RediStar. A Record go through the entire execution plan and in the end returned to the user as a result.

## Reader
The reader is the first component on each execution, The reader responsable for suppling Record to the other components in the execution plan. RediStar comes with a default reader that reads keys from redis. Using python it is possible to use the default reader with the flowing syntax: `starCtx(<key name or prefix>)`, this line will return a star context (which is basicly saves the exectution plan) with the default reader that reads the keys with the given prefix. The default reader will return Records in the following format:
```
{'key':< key as string >, 'value': < value (value type is depend on the key content) >}
```

## Writer
Writer allows to write the records back to the redis. The only writer available today is keyWriter which write a key record to redis. Notice, on cluster you might write keys to the wrong shard, it is important to do repartition before using writer (we will expend the talk about repartition later). Writer example (using python api), the example will write each value as key and each key as value (using python api repartition is automatically):
```
starCtx('test', '*').writeKeys(lambda x: {'key':x['value'], 'value':x['key']}).map(lambda x : str(x)).run()
```

## Operation
Operation is a logic unit that can be applied on the Record. Each operation recieves the Record as input and return result according to the operation type. The supported operation types are:

### Map
Map operation receive a Record and return another Record, example (using python api):
```
ctx.map(lambda r : str(r)) # transform a Record into a string Record
```

### Filter
Filter operation receive a Record and return a boolean indicating whether or not the record should continue with the execution, example (using python api)
```
ctx.filter(lambda r : int(r['value']) > 50) # continue with the record if its value is greater then 50
```

### Groupby
Filter operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key and list of records that grouped together by this key, the reducer return a new Record which is the reduce operation on all the Record in the same group. example (using python api)
```
ctx.groupby(lambda r : r[value], lambda key,vals: len(vals)) # count how many times each value appeared
```

### Collect
Return the results to the initiator (this operation has meaning only on cluster with more then one node, otherwise it has no meaning and it actually do nothing). example (using python api):
```
ctx.collect()
```

# Cluster Support
RediStar support all of the operations on oss-cluster. Notice that the module needs to be loaded on all the cluster nodes. In addition, after setting up the cluster you need to run `rs.refreshcluster` on each node.

## Future plans
* Support more operations: FlatMap, SortBy, LocalGroupBy (for faster groupby on oss-cluster), limit (to limit the number of results).
* StarQL - SQL like language for quering your data.
* StarStreaming - Stream processing on your data.

# License

Apache 2.0 with Commons Clause - see [LICENSE](LICENSE)

