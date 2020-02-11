#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/oracle/rs/setup-oracle.sh
# via: bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-oracle)
# to be executed on a host running the Oracle Docker container

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

BRANCH=write_behind_1

mkdir -p /opt
cd /opt
rm -rf RedisGears recipe || true
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe

/opt/recipe/oracle/install-oracle-docker
/opt/recipe/oracle/install-oracle-client

printf "\n127.0.0.1 oracle\n" >> /etc/hosts

# TIMEOUT=240 /opt/recipe/oracle/rs/wait-for-oracle
# if [[ $? == 0 ]]; then
# 	/opt/recipe/oracle/rs/create-db
# 	echo "Oracle database created."
# else
# 	echo "ERROR: Oracle not ready, database not created."
# 	exit 1
# fi

sleep 240
/opt/recipe/oracle/rs/create-db

exit 0
