# RedisGears
Dynamic execution framework for your Redis data, simply:
```
gearsCtx().filter(filter_function).map(map_function).groupby(key_extractor_function, reducer_function).run('*')
```
RedisGears supports full python syntax and low level c api. In addition you can run it on cluster.

# Quick Start

## Install
### Prerequisites
Install [redis 5.0](https://redis.io/) on you machine.

Libraries: build-essential, autotools-dev, autoconf, libtool

### Build
Init submodules
```
git submodule update --init --recursive
```

Clone the git repository and run:
```
make
```

Notice that RedisGears dynamicly linked with your local python installation, it is possible to run without python support by just typing `make`.

## Run
run: 

```
./scripts/redisd
```

Add some keys to your redis server and then run:
```
127.0.0.1:6379> RG.PYEXECUTE "gearsCtx().map(lambda x:str(x)).run('*')"
```
You will get all of your keys and values in redis.

# RedisGears components
## Execution Plan
RedisGears end goal is to allow the user building Execution Plans and run them. Each execution plan starts with Reader, which supply the data, follow by a chain of operations and ends with run or register (will be explained in details later).

## Record
Record is the most basic object in RedisGears. A Record go through the entire execution plan and in the end returned to the user as a result.

## Reader
The reader is the first component on each execution, The reader responsable for suppling Record to the other components in the execution plan. RedisGears comes with a default reader that reads keys from redis. Using python it is possible to use the default reader with the flowing syntax: `gearsCtx()`, this line will return a gears context (which is basicly saves the exectution plan) with the default reader that reads the keys with the given prefix. The default reader will return Records in the following format:
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
ctx.flatmap(lambda r : [r, r]) # pass each record twice in the execution plan
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
Groupby operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key, accumulator and record, the reducer return a new Record which is the accumulation on all records arrive untill now. example (using python api)
```
ctx.groupby(lambda r : r[value], lambda key,a, r: 1 + (a if a else 0)) # count how many times each value appeared
```

### batchgroupby
BatchGroupby operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key and list of records that grouped together by this key, the reducer return a new Record which is the reduce operation on all the Record in the same group.
It is recommended to use groupby and not batchgroupby. The only reason to use batchgroupby is if you want the reducer to recieve all the records in the group as a list. Notice that in this case the process memory consumption might grow a lot. example (using python api).
example (using python api)
```
ctx.batchgroupby(lambda r : r[value], lambda key, vals: len(vals)) # count how many times each value appeared
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
gearsCtx().repartition(lambda x: x['value']) # repartition record by value
```

### ForEach
ForEach is similar to map but it does not return any value, using ForEach it is possible for example to write results back to redis. After performing the ForEach operation the execution plan will continue with the same records.
example (using python api):
```
gearsCtx().foreach(lambda x: redisgears.execute_command('set', x['value'], x['key'])) # will save value as key and key as value
```

### Accumulate
Accumulate is a many to one mapper, it allows you to accumulate the data to a single record.
example (using python api):
```
gearsCtx().map(lambda x:int(x['value'])).accumulate(lambda a,x: a + x if a is not None else x) # will sum all the values
```

# Cluster Support
RedisGears support all of the operations on oss-cluster. Notice that the module needs to be loaded on all the cluster nodes. In addition, after setting up the cluster you need to run `rs.refreshcluster` on each node.

## Future plans
* Support more operations: SortBy, LocalGroupBy (for faster groupby on oss-cluster), Accumulate, ToList
* GearsQL - SQL like language for quering your data.

# License

Redis Source Available License Agreement - see [LICENSE](LICENSE)

