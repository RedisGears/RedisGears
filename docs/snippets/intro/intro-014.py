def fname(x):
  ''' Extracts the family name from a person's record '''
  return x['value']['name'].split(' ')[1]

def key(x):
  ''' Extracts the key of a record '''
  return x['key']

def counter(k, a, r):
  ''' Counts records '''
  return (a if a else 0) + 1

def summer(k, a, r):
  ''' Sums record values '''
  return (a if a else 0) + r['value']

# Use local and global groupby operations
gb = GearsBuilder()
gb.localgroupby(fname, counter)
gb.groupby(key, summer)
gb.run('person:*')

# Expected result: the same
