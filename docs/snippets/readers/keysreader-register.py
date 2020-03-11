# KeysReader will trigger for keys prefixed by 'bar' of type 'string' or 'hash'
gb = GB('KeysReader')
gb.register('bar', keyTypes=['string', 'hash'])