#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/cassandra/rs/setup-cassandra.sh
# via: bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-cassandra)
# to be executed on a host running the Cassandra Docker container

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

BRANCH=write_behind_1

mkdir -p /opt
cd /opt
rm -rf RedisGears recipe || true
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe

/opt/recipe/cassandra/install-cassandra-cluster

printf "\n127.0.0.1 cassandra\n" >> /etc/hosts

# TIMEOUT=240 /opt/recipe/cassandra/rs/wait-for-cassandra
# if [[ $? == 0 ]]; then
# 	/opt/recipe/cassandra/rs/create-db
# 	echo "Cassandra database created."
# else
# 	echo "ERROR: Cassandra not ready, database not created."
# 	exit 1
# fi

sleep 240
/opt/recipe/cassandra/rs/create-db

exit 0
