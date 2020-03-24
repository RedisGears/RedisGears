def age(x):
  ''' Extracts the age from a person's record '''
  return int(x['value']['age'])

def cas(x):
  ''' Checks and sets the current maximum '''
  k = 'age:maximum'
  v = execute('GET', k)   # read key's current value
  v = int(v) if v else 0  # initialize to 0 if None
  if x > v:               # if a new maximum found
    execute('SET', k, x)  # set key to new value

# Event handling function registration
gb = GearsBuilder()
gb.map(age)
gb.foreach(cas)
gb.register('person:*')

## Expected result: ['OK']
