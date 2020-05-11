# Will put all records of each value in a different list
gb = GB()
gb.aggregateby(lambda x: x['value'],
               [],
               lambda k, a, r: a + [r],
               lambda k, a, r: a + x)
gb.run()
