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

# Repartition for storing counts
gb = GearsBuilder()
gb.localgroupby(fname, counter)
gb.repartition(key)
gb.localgroupby(key, summer)
gb.foreach(lambda x: execute('SET', x['key'], x['value']))
gb.run('person:*')

# Expected result: the same + stored in Redis String keys
