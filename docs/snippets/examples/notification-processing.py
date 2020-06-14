def process(x):
    '''
    Processes a message from the local expiration stream
    Note: in this example we simply print to the log, but feel free to replace
    this logic with your own, e.g. an HTTP request to a REST API or a call to an
    external data store.
    '''
    log(f"Key '{x['value']['key']}' expired at {x['id'].split('-')[0]}")

# Capture an expiration event and adds it to the shard's local 'expired' stream
cap = GB('KeysReader')
cap.foreach(lambda x:
            execute('XADD', f'expired:{hashtag()}', '*', 'key', x['key']))
cap.register(prefix='*',
             mode='sync',
             eventTypes=['expired'],
             readValue=False)

# Consume new messages from expiration streams and process them somehow
proc = GB('StreamReader')
proc.foreach(process)
proc.register(prefix='expired:*',
              batch=100,
              duration=1)