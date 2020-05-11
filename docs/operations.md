# RedisGears Operations

An **operation** is the building block of RedisGears [functions](functions.md). Different operation types can be used to achieve a variety of results to meet various data processing needs.

Operations can have zero or more arguments that control their operation. Depending on the operation's type arguments may be language-native data types and function callbacks.

The following sections describe the different operations.

| Operation | Description | Type |
| --- | --- | --- |
| [Map](#map) | Maps 1:1 | Local |
| [FlatMap](#flatmap) | Maps 1:N | Local |
| [ForEach](#foreach) | Does something for each record | Local |
| [Filter](#filter) | Filters records | Local |
| [Accumulate](#accumulate) | Maps N:1 | Local |
| [LocalGroupBy](#localgroupby) | Groups records by key | Local |
| [Limit](#limit) | Limits the number of records | Local |
| [Collect](#collect) | Shuffles all records to one engine | Global |
| [Repartition](#repartition) | Shuffles records between all engines | Global |
| [GroupBy](#groupby) | Groups records by key | Sugar |
| [BatchGroupBy](#batchgroupby) | Groups records by key | Sugar |
| [Sort](#sort) | Sorts records | Sugar |
| [Distinct](#distinct) | Makes distinct records | Sugar |
| [Aggregate](#aggregate) | Aggregates records | Sugar |
| [AggregateBy](#aggregateby) | Aggregates records by key | Sugar |
| [Count](#count) | Counts records | Sugar |
| [CountBy](#countby) | Counts records by key| Sugar |
| [Avg](#avg) | Computes the average | Sugar |

## Map
The local **Map** operation performs the one-to-one (1:1) mapping of records.

It requires one [mapper](#mapper) callback.

!!! info "Common uses"
    * Transform the data's shape
    * Typecasting and value formatting
    * Splitting, joining and similar string manipulations
    * Removing and/or adding data from/to the record

**Python API**
```python
class GearsBuilder.map(f)
```

_Arguments_

* _f_: the [mapper](#mapper) function callback

**Examples**
```python
{{ include('operations/map.py') }}
```

## FlatMap
The local **FlatMap** operation performs one-to-many (1:N) mapping of records.

It requires one  [expander](#expander) callback that maps a single input record to one or more output records.

FlatMap is nearly identical to the [Map](#map) operation in purpose and use. Unlike regular mapping, however, when FlatMap returns a list, each element in the list is turned into a separate output record.

!!! info "Common uses"
    * Deconstruction of nested, multi-part, or otherwise overly-complicated records

**Python API**
```python
class GearsBuilder.flatmap(f)
```

_Arguments_

* _f_: the [expander](#expander) function callback

**Examples**
```python
{{ include('operations/flatmap.py') }}
```

## ForEach
The local **ForEach** operation performs one-to-the-same (1=1) mapping.

It requires one [processor](#processor) callback to perform some work that's related to the input record.

Its output record is a copy of the input, which means anything the callback returns is discarded.

!!! info "Common uses"
    * Non-transforming, record-related logic

**Python API**
```python
class GearsBuilder.foreach(f)
```

_Arguments_

* _f_: the [processor](#processor) function callback that will be used on each record

**Examples**
```python
{{ include('operations/foreach.py') }}
```

## Filter
The local **Filter** operation performs one-to-zero-or-one (1:(0|1)) filtering of records.

It requires a [filterer](#filterer) function callback.

An input record that yields a falsehood will be discarded and only truthful ones will be output.

!!! info "Common uses"
    * Filtering records

**Python API**
```python
class GearsBuilder.filter(f)
```

_Arguments_

* _f_: the filtering function callback

**Examples**
```python
{{ include('operations/filter.py') }}
```

## Accumulate
The local **Accumulate** operation performs many-to-one mapping (N:1) of records.

It requires one [accumulator](#accumulator) callback.

Once input records are exhausted its output is a single record consisting of the accumulator's value.

!!! info "Common uses"
    * Aggregating records

**Python API**
```python
class GearsBuilder.accumulate(f)
```

_Arguments_

* _f_: an [accumulator](#accumulator) function callback

**Examples**
```python
{{ include('operations/accumulate.py') }}
```

## LocalGroupBy
The local **LocalGroupBy** operation performs many-to-less mapping (N:M) of records.

The operation requires two callbacks: an [extractor](#extractor) a [reducer](#reducer).

The output records consist of the grouping key and its respective accumulator's value.

!!! info "Common uses"
    * Grouping records by key

**Python API**
```python
class GearsBuilder.localgroupby(e, r)
```

_Arguments_

* _e_: a key [extractor](#extractor) function callback
* _r_: a [reducer](#reducer) function callback

**Examples**
```python
{{ include('operations/localgroupby.py') }}
```

## Limit
The local **Limit** operation limits the number of records.

It accepts two numeric arguments: a starting position in the input records "array" and a maximal number of output records.

!!! info "Common uses"
    * Returning the first results
    * Batch paging on static data

**Python API**
```python
class GearsBuilder.limit(length, start=0)
```

_Arguments_

* _length_: the maximal length of the output records list
* _start_: a 0-based index of the input record to start from

**Examples**
```python
{{ include('operations/limit.py') }}
```

## Collect
The global **Collect** operation collects the result records from all of the shards to the originating one.

It has no arguments.

!!! info "Common uses"
    * Final steps of distributed executions

**Python API**
```python
class GearsBuilder.collect()
```

**Examples**
```python
{{ include('operations/collect.py') }}
```

## Repartition
The global **Repartition** operation repartitions the records by them shuffling between shards.

It accepts a single key [extractor](#extractor) function callback. The extracted key is used for computing the record's new placement in the cluster (i.e. hash slot). The operation then moves the record from its original shard to the new one.

!!! info "Common uses"
    * Remapping of records to engines
    * JOIN-like operations

**Python API**
```python
class GearsBuilder.repartition(f)
```

_Arguments_

* _f_: a key [extractor](#extractor) function callback

**Examples**
```python
{{ include('operations/repartition-001.py') }}
```

```python
{{ include('operations/repartition-002.py') }}
```

## Aggregate
The sugar **Aggregate** operation performs many-to-one mapping (N:1) of records.

Aggregate provides an alternative to the local [accumulate](#accumulate) operation as it takes the partitioning of data into consideration. Furthermore, because records are aggregated locally before collection, its performance is usually superior.

It requires a zero value and two [accumulator](#accumulator) callbacks for computing the local and global aggregates.

The operation is made of these steps:

  1. The local [accumulator](#accumulator) is executed locally and initialized with the zero value
  1. A global [collect](#operation) moves all records to the originating engine
  1. The global [accumulator](#accumulator) is executed locally by the originating engine

Its output is a single record consisting of the accumulator's global value.

**Python API**
```python
class GearsBuilder.aggregate(z, l, g)
```

_Arguments_

* _z_: the aggregate's zero value
* _l_: a local [accumulator](#accumulator) function callback
* _g_: a global [accumulator](#accumulator) function callback

**Examples**
```python
{{ include('operations/aggregate.py') }}
```

## AggregateBy
The sugar **AggregateBy** operation performs many-to-less mapping (N:M) of records.

It is similar to the [Aggregate](#aggregate) operation but aggregates per key. It requires a an [extractor](#extractor) callback, a zero value and two [reducers](#reducer) callbacks for computing the local and global aggregates.

The operation is made of these steps:

  1. extraction of the groups using [extractor](#extractor)
  1. The local [reducer](#reducer) is executed locally and initialized with the zero value
  1. A global [repartition](#repartition) operation that uses the [extractor](#extractor)
  1. The global [reducer](#reducer) is executed on each shard once it is repartitioned with its relevant keys

Output list of records, one for each key. The output records consist of the grouping key and its respective reducer's value.

**Python API**
```python
class GearsBuilder.aggregateby(e, z, l, g)
```

_Arguments_

* _e_: a key [extractor](#extractor) function callback
* _z_: the aggregate's zero value
* _l_: a local [reducer](#reducer) function callback
* _g_: a global [reducer](#reducer) function callback

**Examples**
```python
{{ include('operations/aggregateby.py') }}
```

## GroupBy
The sugar *GroupBy** operation performs a many-to-less (N:M) grouping of records. It is similar to [AggregateBy](#aggregateby) but uses only a global reducer. It can be used in cases where locally reducing the data isn't possible.

The operation requires two callbacks: an [extractor](#extractor) a [reducer](#reducer).

The operation is made of these steps:

  1. A global [repartition](#repartition) operation that uses the [extractor](#extractor)
  1. The [reducer](#reducer) is locally invoked

Output is a locally-reduced list of records, one for each key. The output records consist of the grouping key and its respective accumulator's value.

**Python API**
```python
class GearsBuilder.groupby(e, r)
```

_Arguments_

* _e_: a key [extractor](#extractor) function callback
* _r_: a [reducer](#reducer) function callback

**Examples**
```python
{{ include('operations/groupby.py') }}
```

## BatchGroupBy
The sugar **BatchGroupBy** operation performs a many-to-less (N:M) grouping of records.

!!! important "Prefer the GroupBy Operation"
    Instead of using BatchGroupBy, prefer using the [GroupBy](#groupby) operation as it is more efficient and performant. Only use BatchGroupBy when the reducer's logic requires the full list of records for each input key.

The operation requires two callbacks: an [extractor](#extractor) a [batch reducer](#batch-reducer).

The operation is made of these steps:

  1. A global [repartition](#repartition) operation that uses the [extractor](#extractor)
  1. A local [localgroupby](#localgroupby) operation that uses the [batch reducer](#batch-reducer)

Once finished, the operation locally outputs a record for each key and its respective accumulator value.

!!! warning "Increased memory consumption"
    Using this operation may cause a substantial increase in memory usage during runtime.

**Python API**
```python
class GearsBuilder.batchgroupby(e, r)
```

_Arguments_

* _e_: a key [extractor](#extractor) function callback
* _r_: a [batch reducer](#batch-reducer) function callback

**Examples**
```python
{{ include('operations/batchgroupby.py') }}
```

## Sort
The sugar **Sort** operation sorts the records.

It accepts a single Boolean argument that determines the order.

The operation is made of the following steps:

  1. A global [aggregate](#aggregate) operation collects and combines all records
  1. A local sort is performed on the list
  1. The list is [flatmapped](#flatmap) to records

!!! warning "Increased memory consumption"
    Using this operation may cause an increase in memory usage during runtime due to the list being copied during the sorting operation.

**Python API**
```python
class GearsBuilder.sort(reverse=True)
```

_Arguments_

* _reverse_: when `True` sorts in descending order

**Examples**
```python
{{ include('operations/sort.py') }}
```

## Distinct
The sugar **Distinct** operation returns distinct records.

It requires no arguments.

The operation is made of the following steps:

  1. A [aggregate](#aggregate) operation locally reduces the records to sets that are then collected and unionized globally
  2. A local [flatmap](#flatmap) operation turns the set into records

**Python API**
```python
class GearsBuilder.distinct()
```

**Examples**
```python
{{ include('operations/distinct.py') }}
```

## Count
The sugar **Count** operation counts the records.

It requires no arguments.

The operation is made of an [aggregate](#aggregate) operation that uses local counting and global summing accumulators.

**Python API**
```python
class GearsBuilder.count()
```

**Examples**
```python
{{ include('operations/count.py') }}
```

## CountBy
The sugar **CountBy** operation counts the records grouped by key.

It requires a single [extractor](#extractor) function callback.

The operation is made of an [aggregateby](#aggregateby) operation that uses local counting and global summing accumulators.

**Python API**
```python
class GearsBuilder.countby(extractor=lambda x: x)
```

_Arguments_

* _extractor_: an optional key [extractor](#extractor) function callback

**Examples**
```python
{{ include('operations/countby.py') }}
```

## Avg
The sugar **Avg** operation returns the arithmetic average of records.

It accepts an optional value [extractor](#extractor) function callback.

The operation is made of the following steps:

  1. A [aggregate](#aggregate) operation locally reduces the records to tuples of sum and count that are globally combined.
  2. A local [map](#map) operation calculates the average from the global tuple

**Python API**
```python
class GearsBuilder.avg(extractor=lambda x: float(x))
```

_Arguments_

* _extractor_: an optional value [extractor](#extractor) function callback

**Examples**
```python
{{ include('operations/avg.py') }}
```

## Terminology

### Local
The **Local** execution of an operation is carried out the RedisGears engine that's deployed in either _stand-alone_ or _cluster_ mode. When used alone, there's a single engine executing all operations on all data locally.

When clustered, the operation is distributed to all shards. Each shard's engine executes the operation locally as well. Shards' engines, however, can only process the data they are partitioned with by the cluster.

### Global
**Global** operations are only relevant in the context of a clustered RedisGears environment.
These are the [Collect](#collect) and [Repartition](#repartition) operations that shuffle records between shards.

### Sugar
A **Sugar** operation is a utility operation. These are implemented internally with basic operations and the relevant callbacks.

### Callback
A **Callback** is used for calling a function in the language used by the API.

### Extractor
An **Extractor** is a callback that receives an input record as an argument. It returns a value extracted from the record. The returned value should be a native string.

**Python**
```python
# Lambda function form
lambda r: str(...)

# Function form
def extractorFunction(r):
  ...
  return ...
```

_Arguments_

* _r_: the input record


**Examples**
```python
# These extractors expect dict() records having a 'key' key (e.g. KeysReader)
def keyExtractor(r):
  ''' Just extracts the key '''
  return str(r['key'])

def reverseExtractor(r):
  ''' Reverses the extracted key '''
  return str(r['key'])[::-1]

# This extractor expects dict() records having a string 'value' key (e.g. KeysReader with Redis Strings)
def floatExtractor(r):
  ''' Makes the value float '''
  return float(r['value'])
```

### Mapper
A **Mapper** is a callback that receives an input record as an argument. It must return an output record.

**Python**
```python
# Lambda function form
lambda r: ...

# Function form
def mapperrFunction(r):
  ...
  return o
```

_Arguments_

* _r_: the input record

_Return_

* _o_: an output record

**Examples**
```python
# This mapper expects dict() records having a 'key' key (e.g. KeysReader)
def keyOnlyMapper(r):
  ''' Maps a record to its key only '''
  return str(r['key'])
```

### Expander
An **Expander** is a callback that receives an input record. It must return one or one or more output records.

**Python**
```python
# Lambda function form
lambda r: list(...)

# Function form
def expanderFunction(r):
  ...
  return list(i)
```

_Arguments_

* _r_: the input record

_Return_

* _i_: an iterable of output records

**Examples**
```python
# This expander expects KeysReader records of Redis Hashes
def hashExploder(r):
  ''' Splats a record's dict() 'value' into its keys '''
  # Prefix each exploded key with the original in curly brackets and a colon
  # for clustering safety, i.e.: {hashkeyname}:fieldname
  pre = '{' + r['key'] + '}:'
  l = [{ 'key': f'{pre}{x[0]}', 'value': x[1] } for x in r['value'].items()]
  return l
```

### Processor
A **Processor** is a callback that receives an input record. It shouldn't return anything.

**Python**
```python
# Lambda function form
lambda r: ...

# Function form
def processorFunction(r):
  ...
```

_Arguments_

* _r_: the input record

**Examples**
```python
def logProcessor(r):
  ''' Log each record '''
  log(str(r))
```


### Filterer
A **Filterer** is a callback that receives an input record. It must return a Boolean value.

**Python**
```python
# Lambda function form
lambda r: bool(...)

# Function form
def filtererFunction(r):
  ...
  return bool(b)
```

_Arguments_

* _r_: the input record

_Return_

* _b_: a Boolean value

**Examples**
```python
def dictRecordFilter(r):
  ''' Filters out non-dict records (e.g. Redis' Strings won't pass) '''
  return type(r) is dict
```

### Accumulator
An **Accumulator** is a callback that receives an input record and variable that's also called an accumulator. It aggregates inputs into the accumulator variable, which stores the state between the function's invocations. The function must return the accumulator's updated value after each call.

**Python**
```python
# Lambda function form
lambda a, r: ...

# Function form
def accumulatorFunction(a, r):
  ...
  return u
```

_Arguments_

* _a_: the accumulator's value from previous calls
* _r_: the input record

_Return_

* _u_: the accumulator's updated value

**Examples**
```python
# This accumulator expects nothing
def countingAccumulator(a, r):
  ''' Counts records '''
  # a's initial value is None so set it to a zero value if so
  a = a if a else 0
  # increment it by one
  a = a + 1
  return a
```

### Reducer
A **Reducer** is a callback function that receives a key, an input and a variable that's called an accumulator. It performs similarly to the [accumulator](#accumulator) callback, with the difference being that it maintains an accumulator per reduced key.

**Python**
```python
# Lambda function form
lambda k, a, r: ...

# Function form
def reducerFunction(k, a, r):
  ...
  return u
```

_Arguments_

* _k_: the key
* _a_: the accumulator's value from previous calls
* _r_: the input record

_Return_

* _u_: the accumulator's updated value

**Examples**
```python
def keyCountingReducer(k, a, r):
  ''' Counts records for each key'''
  # a's initial value is None so set it to a zero value if so
  a = a if a else 0
  # increment it by one
  a = a + 1
  return a
```

### Batch Reducer
A **Batch Reducer** is a callback function that receives a key and a list of input records. It performs similarly to the [reducer](#reducer) callback, with the difference being that it is input with a list of records instead of a single one. It is expected to return an accumulator value for these records.

**Python**
```python
# Lambda function form
lambda k, l: ...

# Function form
def batchReducerFunction(k, l):
  ...
  return a
```

_Arguments_

* _k_: the key
* _l_: the list of input record

_Return_

* _a_: the accumulator's value

**Examples**
```python
def batchKeyCountingReducer(k, l):
  ''' Counts records for each key'''
  a = len(l)
  return a
```
