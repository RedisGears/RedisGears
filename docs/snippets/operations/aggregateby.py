# Will put all records of each value in a different list
gb = GB()
gb.aggregateby(lambda x: x['value'],
               [],
               lambda a, r: a + [r],
               lambda a, r: a + x)
gb.run()
