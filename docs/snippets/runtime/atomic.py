# Increments two keys atomically
def transaction(_):
    with atomic():
        execute('INCR', f'{{{hashtag()}}}:foo')
        execute('INCR', f'{{{hashtag()}}}:bar')

gb = GB('ShardsIDReader')
gb.foreach(transaction)
gb.run()