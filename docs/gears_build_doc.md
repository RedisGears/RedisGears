# GearsBuilder Documentation

```
execute(*args):
	Execute a command directly on the redis

class GearsBuilder
 |  Methods defined here:
 |  
 |  __init__(self, reader='KeysReader', defautlPrefix='*')
 |		reader - the reader to use (currently : 'KeysReader' / 'StreamReader')
 |		defautlPrefix - the default prefix of the keys on which to run the execution (might be overide on the 'run' function)
 |
 |  map(mapFucntio):
 |		Maping one record to another using the mapping function
 |
 |  flatmap(self, mapFunction):
 |		One to many mapper, gets a record and return list for records. Each record in the list will continue the execution on its own.
 |
 |  foreach(self, function):
 |		Like map, but not returning any value, after performing the function the record will continue with the exection.
 |
 |  filter(self, filterFunction):
 |		Pass each record through the filterFunction, if True is return the record will continue the execution, otherwise its droped.
 |
 |  count(self)
 |      Count the number of recors in the execution
 |
 |  avg(self, extractor=<function <lambda>>)
 |      Calculating average on all the records
 |      extractor - a function that gets the record and return the value by which to calculate the average
 |
 |  distinct(self)
 |      Keep only the distinct values in the data
 |
 |  limit(self, num, offset):
 |		Return num records starting from a given offset.
 |
 |  sort(self, reverse=True):
 |      Sorting the data
 |
 |  aggregate(self, zero, seqOp, combOp)
 |      perform aggregation on all the execution data.
 |      zero - the first value that will pass to the aggregation function
 |      seqOp - the local aggregate function (will be performed on each shard), receive the aggregated object and the record, returns new aggregated object.
 |      combOp - the global aggregate function (will be performed on the results of seqOp from each shard), receive the aggregated object and the record, returns new aggregated object.
 |  
 |  aggregateby(self, extractor, zero, seqOp, combOp)
 |      Like aggregate but on each key, the key is extracted using the extractor.
 |      extractor - a function that get as input the record and return the aggregated key
 |      zero - the first value that will pass to the aggregation function
 |      seqOp - the local aggregate function (will be performed on each shard)
 |      combOp - the global aggregate function (will be performed on the results of seqOp from each shard)
 |
 |  accumulate(self, accumulator)
 |		Like aggregate but with out zero value. The first value will be 'None'
 |      accumulator - function that gets the accumulated object and the record and should return a new accumulated object
 |  
 |  countby(self, extractor=<function <lambda>>)
 |      Count, for each key, the number of recors contains this key.
 |      extractor - a function that get as input the record and return the key by which to perform the counting
 |
 |  groupby(self, extractor, reducer):
 |		Like aggregate by but with out a zero value and local aggregator. For performance its better to use aggregate by.
 |		Perform groupby + reduce, the groupby is performed by the return value from the extractor. The reducer is a function
 |		that recieve the key, aggregator, record and return new aggregator.
 |
 |  batchgroupby(self, extractor, reducer):
 |		Like groupby but the reducer will recieve the list of all the values belong to the group. This fanction might increase memory usage
 |  
 |  collect(self):
 |		Retrun all the values to the initiator (the shard that started the execution)
 |
 |  repartition(self, extractor):
 |		Repartition the data by the extracted value
 |  
 |  register(self, key):
 |		Register the execution on event
 |  
 |  run(self, prefix=None, converteToStr=True, collect=True)
 |      Starting the execution
 |

```