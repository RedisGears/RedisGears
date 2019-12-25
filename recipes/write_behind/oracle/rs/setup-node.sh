#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/rafi-behind_oracle-1/recipes/oracle/rs/setup-node.sh

if [[ -z $ORACLE]]; then
	echo "Error: no ORACLE IP address given. Aborting."
	exit 1
fi
	
BUCKET=https://s3.amazonaws.com/redismodules/lab/08-gears-write-behind
DIR=/opt/redisgears-setup
BRANCH=rafi_behind-oracle-1

mkdir -p $DIR
wget -q -O $DIR/install-modules.py $BUCKET/install-modules.py
wget -q -O $DIR/redis-modules.yaml $BUCKET/redis-modules.yaml
/opt/redislabs/bin/python2 $DIR/install-modules.py --yaml $DIR/redis-modules.yaml

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RegisGears/RedisGears.git

ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd recipe
ln -s ../gear.py

echo "$ORACLE oracle" >> /etc/hosts

/opt/recipe/oracle/install-oracle-client
/opt/recipe/oracle/install-oracle-python-client

exit 0
