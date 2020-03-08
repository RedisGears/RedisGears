def maximum(a, x):
  ''' Returns the maximum '''
  a = a if a else 0       # initialize the accumulator
  return max(a, x)

# Reduced maximum function
gb = GearsBuilder()
gb.map(lambda x: int(x['value']['age']))
gb.accumulate(maximum)
gb.collect()
gb.accumulate(maximum)
gb.run('person:*')

## Expected result: [87]
