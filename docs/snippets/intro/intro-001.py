# A utility function
def isperson(x):
    ''' Checks if the record's key looks like a person '''
    return x['key'].startswith('person:')

# A RedisGears function
gb = GB()
gb.filter(isperson)
gb.run()
