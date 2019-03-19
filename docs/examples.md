# RedisGears Streaming

## Word Count
Assuming your data are located in redis keys, each key is a sentence. It is possible to use Gear script to count how many unique words there is.
```
# creating gears builder
bg = GearsBuilder()

# getting the value from each key
bg.map(lambda x: x['value'])

# split each line to words
bg.flatmap(lambda x: x.split())

# count for each word how many times it appears
bg.countby()

# starting the execution
bg.run()
```


## Delete Keys by Prefix
Delete all the keys that starts with `city:`
```
# creating gears builder
bg = GearsBuilder()

# getting the key name
bg.map(lambda x: x['key'])

# split each line to words
bg.foreach(lambda x: execute('del', x))

# count how many keys was deleted
bg.count()

# starting the execution on 'city:*'
bg.run('city:*')
```

## Stream Processing
Put each record that enter stream `s1` into a hash
```
# creating gears builder
bg = GearsBuilder('StreamReader')

# Set the data in the hash
bg.foreach(lambda x: execute('hmset', x['streamId'], *x))

# register the execution on `s1`
bg.register('s1')
```

