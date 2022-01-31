def my_hset(r):
    execute('hincrby', r[1], '_times_modified_', 1)
    return call_next(*r[1:])

GB('CommandReader').map(my_hset).register(hook='hset', mode='sync')
