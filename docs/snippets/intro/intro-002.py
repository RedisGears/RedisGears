gb = GearsBuilder()                       # declare a function builder
gb.map(lambda x: int(x['value']['age']))  # map each record to just an age
gb.run('person:*')                        # run it

## Expected result: [70, 14]
