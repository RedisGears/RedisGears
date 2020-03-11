def maximum(a, x):
  ''' Returns the maximum '''
  a = a if a else 0       # initialize the accumulator
  return max(a, x)

# Original, non-reduced, maximum function version
gb = GearsBuilder()
gb.map(lambda x: int(x['value']['age']))
gb.accumulate(maximum)
gb.run('person:*')

## Expected result: [87, 35, 14]
