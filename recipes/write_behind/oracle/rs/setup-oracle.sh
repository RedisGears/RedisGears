#!/bin/bash

# https://raw.githubusercontent.com/RedisGears/RedisGears/master/recipes/write_behind/oracle/rs/setup-oracle.sh

set -e

BRANCH=master

mkdir -p /opt
cd /opt
git clone --branch $BRANCH --single-branch https://github.com/RedisGears/RedisGears.git
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe

/opt/recipe/oracle/install-oracle-docker
/opt/recipe/oracle/install-oracle-client

printf "\n127.0.0.1 oracle\n" >> /etc/hosts

/opt/recipe/oracle/rs/create-db

exit 0
