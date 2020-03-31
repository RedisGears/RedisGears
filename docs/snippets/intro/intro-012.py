# Aggregated average
gb = GearsBuilder()
gb.map(lambda x: int(x['value']['age']))
gb.aggregate((0, 0),
             lambda a, x: (a[0] + x, a[1] + 1),
             lambda a, x: (a[0] + x[0], a[1] + x[1]))
gb.map(lambda x: x[0]/x[1])
gb.run('person:*')

## Expected result: [44.6]
