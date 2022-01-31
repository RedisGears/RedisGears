import redis

def fetch_data(r):
    key = r['key']
    conn = redis.Redis('localhost', 6380)
    data = conn.get(key)
    execute('set', key, data)
    override_reply(data)

GB().foreach(fetch_data).register(commands=['get'], eventTypes=['keymiss'], mode="async_local")