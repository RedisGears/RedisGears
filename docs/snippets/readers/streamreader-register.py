# StreamReader will be triggered on new messages in 'iot:'-prefixed keys
gb = GB('StreamReader')
gb.register('iot:*')