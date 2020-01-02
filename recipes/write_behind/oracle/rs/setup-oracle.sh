#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/oracle/rs/setup-oracle.sh

set -e

BRANCH=write_behind_1

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe

/opt/recipe/oracle/install-oracle-docker
/opt/recipe/oracle/install-oracle-client

printf "\n127.0.0.1 oracle\n" >> /etc/hosts

TIMEOUT=120 /opt/recipe/oracle/rs/wait-for-oracle && /opt/recipe/oracle/rs/create-db

exit 0
