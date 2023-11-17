def my_hset(r):
    t = str(execute('time')[0])
    new_args = r[1:] + ['_last_modified_', t]
    return call_next(*new_args)

GB('CommandReader').map(my_hset).register(hook='hset', mode='sync')