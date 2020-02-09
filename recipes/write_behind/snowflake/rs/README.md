# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster for Snowflake DB

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on Ubuntu Xenial/Ubuntu Bionic/RHEL 7
* Snowflake DB account (you'll need an account name, username and password)
* RedisGears module for a matching platform

## Installing the Redis cluster

* [Create a Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).
* On each cluster node, run (as root, via `sudo bash`) - fill your account code and credentials:

```
SNOW_USER="..." SNOW_PASSWD="..." SNOW_ACCT="CODE.eu-west-1" \
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node-snowflake)
```
* Download the Redis Gears module and add it to the cluster modules list.
	* For Ubuntu Xenial, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-xenial-x64.99.99.99-3e6d45a.zip).
	* For Ubuntu Bionic, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-bionic-x64.99.99.99-3e6d45a.zip).
	* For RHEL7, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-centos7-x64.99.99.99-3e6d45a.zip).

* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.
	* Add the following parameters, for sample DB configuration: `WriteBehind:dbtype snowflake WriteBehind:db test WriteBehind:user user WriteBehind:passwd passwd`
* Create a Snowflake database using the following script:
```
/opt/recipe/snowflake/rs/create-exmaple-db
```

## Running the write-behind gear

On one of the Redis cluster nodes:

* Run `/opt/recipe/rs/start-gear`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/rs/start-gear`.

### Basic tests
If you created the example database, you can run the following tests to verify if your setup is working correctly.

* From within `bdb-cli <db-id>`, `RG.DUMPREGISTRATIONS` will return a list of registrations.
* Using `bdb-cli <db-id>`, invoke:
```
HSET person2:johndoe first_name "John" last_name "Doe" age "42"
```
* Verify a record was created on Snowflake. Using ```snowsql``` invoke:
```
select * from person1;
```

## Testing
* Log on via SSH to a cluster node.
* Run `/opt/recipe/rs/run-test`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/rs/run-test`.
* Open another connection to that node and run `/opt/recipe/snowflake/sample-snowsql-db`

## Diagnostics

### Redis status

* `rladmin status` command
* Redis configuration files at `/var/opt/redislabs/redis`
* Redis log at `/var/opt/redislabs/log/redis-#.log`
* Restart Redis shards (do that to restart Gears):
```
rlutil redis_restart redis=<Redis shard IDs> force=yes
```

### Gears status

* `redis-cli via bdb-cli <db-id>`
  * `RG.DUMPEXECUTIONS` command

### Snowflake status

* `/opt/recipe/snowflake/sample-snowsql-db` will repeatedly print number of records in the Snowflake test table.

* Snowflake CLI: `snowsql` 

## Configure the gear to reflect your database schema
TBD

