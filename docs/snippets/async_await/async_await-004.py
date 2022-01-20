async def AsyncCountStudents():
	c = await GB().count().run(prefix='student:*')
	if len(c[1]) > 0:
		# we have an errors.
		return c[1]
	# we have only one result which is the count
	if len(c[0]) == 0:
		c = 0 # zero results
	else:
		c = c[0][0]

	# cache the count for 5 seconds
	execute('set', 's_count{%s}' % hashtag(), c, 'EX', '5')
	return c

def CountStudents(r):
	c = execute('get', 's_count{%s}' % hashtag())
	if c:
		return c
	return AsyncCountStudents()

GB('CommandReader').map(CountStudents).register(trigger='COUNT_STUDENTS1', mode='sync')
