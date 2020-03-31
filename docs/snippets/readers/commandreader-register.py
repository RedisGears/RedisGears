# This CommandReader will return the number of arguments provided to it
gb = GB('CommandReader')
gb.map(lambda x: f'{x[0]} got {len(x)-1} arguments! Ah ah ah!')
gb.register(trigger='CountVonCount')
