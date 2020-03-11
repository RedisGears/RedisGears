def fname(x):
  ''' Extracts the family name from a person's record '''
  return x['value']['name'].split(' ')[1]

# Count family members
gb = GearsBuilder()
gb.countby(fname)
gb.run('person:*')

# Expected result: [
#   {'key': 'Pibbles', 'value': 1},
#   {'key': 'Smith', 'value': 3},
#   {'key': 'Sanchez', 'value': 1}
# ]
