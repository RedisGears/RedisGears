# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster for Snowflake DB

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on RHEL7
* Snowflake DB account (you'll need an account name, username and password)
* RedisGears module built for RHEL7/CentOS7

## Installing the Redis cluster

* [Create a Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).
* On each cluster node, run (as root, via `sudo bash`) - fill your account code and credentials:

```
SNOW_USER="..." SNOW_PASSWD="..." SNOW_ACCT="CODE.eu-west-1" \
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node-snowflake)
```

* Download the [Redis Gears module](http://redismodules.s3.amazonaws.com/lab/11-gears-write-behind-sf/redisgears.linux-centos7-x64.99.99.99.zip) and add it to the cluster modules list.
* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.  No special configuration is required.
* Create a Snowflake database using the following script:
```
/opt/recipe/snowflake/rs/create-exmaple-db
```

## Running the write-behind gear

On one of the Redis cluster nodes:

* Run `ID=<db-id> /opt/recipe/snowflake/rs/start-gear`.

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
* Examine `/var/opt/redislabs/redis` for Redis servers running on the node.
  * You can also examine `ps ax | grep redis-server` for that perpose.
* With one of the Redis IDs above, run `ID=<db-id> /opt/recipe/snowflake/rs/run-test`.
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

* redis-cli via bdb-cli DB-ID
  * `RG.DUMPEXECUTIONS` command

### Snowflake status

* `/opt/recipe/snowflake/sample-snowsql-db` will repeatedly print number of records in the Snowlake test table.

* Snowflake CLI: `snowsql` 

## Configure the gear to reflect your database schema
TBD

