#!/opt/redislabs/bin/python2 -O

import os
import CCS
import json

ccs = CCS.Context()
bdb_ids  = ccs.get_bdb_map().keys()
if len(bdb_ids) < 1:
    print("No databases found. Aborting.")
    exit(1)
bdb_id = os.getenv("DB")
if bdb_id is None:
    if len(bdb_ids) == 1:
        bdb_id = bdb_ids[0]
    else:
        print("Please specify database ID (e.g. DB=id)")
        exit(0)
bdb = ccs.get_bdb(bdb_id)
pwd = bdb.authentication_redis_pass()
ep = bdb.get_endpoint_map().values()[0]
port = ep.port()
node_uid = list(ep.proxy_uids())[0]
node = ccs.get_node(node_uid)
ip = node.addr()
print(json.dumps({'host': ip, 'port': port, 'passwd': pwd, 'bdb_name': bdb.name()}))
exit(0)
