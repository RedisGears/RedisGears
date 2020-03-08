# Aggregated maximum version
gb = GearsBuilder()
gb.map(lambda x: int(x['value']['age']))
gb.aggregate(0,
             lambda a, x: max(a, x),
             lambda a, x: max(a, x))
gb.run('person:*')

## Expected result: [87]
