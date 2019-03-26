# RedisGears Overview
RedisGears is a fast cluster computing system. It provides high-level APIs using Python, and a low level api using C.

RedisGears end goal is to allow the user to build an operations pipe (OPP) in which each key in redis will pass through. Results from the first operation will pass as input to the second operation, Results from the second operation will pass as input to the third operation, and so on. Results from the last operation will pass to the user as a reply. The pipe builds using a python script and then runs in background thread. When finished, the results return to the user.

In order to create this OPP, Gears introduce a python class called `GearsBuilder`. When the `GearsBuilder` is first created it contains empty OPP. `GearsBuilder` provide a set of function that allows the user to add operations to the OPP.

example:
The following will create an OPP with one map operation that get all keys from redis.
```
GearsBuilder().map(lambda x: x['key']).run()
```

In addition Gears provide a simple command that allow to pass this python code to the redis server:
```
RG.PYEXECUTE "GearsBuilder().map(lambda x: x['key']).run()"
```

## Record
Record is the most basic object in RedisGears. A Record go through the entire OPP and in the end returned to the user as a result.

## Reader
The reader is the first component on each OPP, The reader responsable for suppling data to be process by the operations in the OPP. RedisGears comes with a default reader that reads keys from redis. The default reader will return Records in the following format:
```
{'key':< key as string >, 'value': < value (value type is depend on the key content) >}
```

By default, when creating a `GearsBuilder`, the builder will automatically use the default reader. It is possible to tell the builder to use different readers, for example:
```
RG.PYEXECUTE "GearsBuilder('KeysOnlyReader').run()" # will read only keys names without the value
RG.PYEXECUTE "GearsBuilder('StreamReader', 's1').run()" # read all the records from stream s1
```


## Operations

### Map
Map operation receive a Record and return another Record, example:
```
GearsBuilder.map(lambda r : str(r)) # transform a Record into a string Record
```

### FlatMap
Just like map but if the result is a list it flatten it right after, example:
```
GearsBuilder.flatmap(lambda r : [r, r]) # pass each record twice in the execution plan
```

### Limit
Limit the number of return results, limit operation gets the offset and the len and return result start from offset and up to len. Notice, limit is a single shard operation, if you want to limit the total number of the results you need to use limit then collect and then limit again. example:
```
GearsBuilder.limit(1, 3) # will return results 1 and 2.
```

### Filter
Filter operation receive a Record and return a boolean indicating whether or not the record should continue with the execution, example:
```
GearsBuilder.filter(lambda r : int(r['value']) > 50) # continue with the record if its value is greater then 50
```

### Groupby
Groupby operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key, accumulator and record, the reducer return a new Record which is the accumulation on all records arrive untill now. example:
```
GearsBuilder.groupby(lambda r : r[value], lambda key,a, r: 1 + (a if a else 0)) # count how many times each value appeared
```

### batchgroupby
BatchGroupby operation receive extractor and reducer. The extractor is a function that receive a Record and return a string by which the group operation need to be performed. The reducer is a function that receive key and list of records that grouped together by this key, the reducer return a new Record which is the reduce operation on all the Record in the same group.
It is recommended to use groupby and not batchgroupby. The only reason to use batchgroupby is if you want the reducer to recieve all the records in the group as a list. Notice that in this case the process memory consumption might grow a lot. example (using python api).
example:
```
GearsBuilder.batchgroupby(lambda r : r[value], lambda key, vals: len(vals)) # count how many times each value appeared
```

### Collect
Return the results to the initiator (this operation has meaning only on cluster with more then one node, otherwise it has no meaning and it actually do nothing). example:
```
GearsBuilder.collect()
```

### Repartition
Repartition the record between the cluster nodes (this operation has meaning only on cluster with more then one node, otherwise it has no meaning and it actually do nothing). This operation receives an extractor, the repartition is perfomed by calculating the hslot on the extracted data and then move the record to the node hold this hslot.
example:
```
GearsBuilder().repartition(lambda x: x['value']) # repartition record by value
```

### ForEach
ForEach is similar to map but it does not return any value, using ForEach it is possible for example to write results back to redis. After performing the ForEach operation the execution plan will continue with the same records.
example:
```
GearsBuilder().foreach(lambda x: redisgears.execute_command('set', x['value'], x['key'])) # will save value as key and key as value
```

### Accumulate
Accumulate is a many to one mapper, it allows you to accumulate the data to a single record.
example:
```
GearsBuilder().map(lambda x:int(x['value'])).accumulate(lambda a,x: a + x if a is not None else x) # will sum all the values
```

### Count
Count return the number of records (notice that on cluster it automatically collect the data from all the shards).
example:
```
GearsBuilder().count().run() # will return the number of keys in redis
```

### CountBy
Count by a spacific field in the data (notice that on cluster it automatically collect the data from all the shards).
example:
```
GearsBuilder().countby(lambda x:x['value']).run() # will return for each value how many times it appears
```

### Sort
Sorting the records. Notice that this operation might increase memory usage because it require saving the entire data to list. In addition, on cluster, it automatically collect the data from all the shards.
example:
```
GearsBuilder().sort().run() # will return all the data sorted
```

### Distinct
Will return only distinct records (notice that on cluster it automatically collect the data from all the shards).
example:
```
GearsBuilder().distinct().run() # will return all the distinct records
```

### Aggregate
A better version of accumulate that recieve a local aggregator (aggregator which will be performed on each shard localy), and a global aggregator (will be performed on the aggregated data collected from each shard). Using aggregate it is possible to increasing perfromance by reducing the numeber of records sent between the shards. In addition aggregate recieve, as the first parameter, the zero value which will pass to the aggregate function on the first time it executed.
example:
```
GearsBuilder().aggregate([], lambda a,x: a + [x], lambda a,x:a + x).run() # will put all values on a single python list
```

### AggregateBy
Like aggregate but provide the abbility to aggregate by a value
example:
```
GearsBuilder().aggregateby(lambda x:x['value'], [], lambda a,x: a + [x], lambda a,x:a + x).run() # will put all records of each value in different list.
```