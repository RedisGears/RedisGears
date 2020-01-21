# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster for Snowflake DB

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on RHEL7
* Snowflake DB account (you'll need an account name, username and password)
* RedisGears module built for RHEL7/CentOS7

## Configuration

TBD: key names and tables

## Installing the Redis cluster

* [Create an un-bootstrapped Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).

* On each cluster node, run (as root, via `sudo bash`):
```
SNOW_USER="..." SNOW_PASSWD="..." SNOW_ACCT="CODE.eu-west-1" \
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node-snowflake)
```

* [Bootstrap the Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/administering/cluster-operations/new-cluster-setup/):
```
/opt/redislabs/bin/rladmin cluster create name cluster1 username a@a.com password a
```

* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.  No special configuration is required.

* Create a Snowflake database using `/opt/recipe/snowflake/rs/create-exmaple-db`. This will set up a user and tables for testing.

## Configure the gear to reflect your database schema
<<TODO>>

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
<<TODO do we need this ??? can we point to a more general "testing your gear documentation">>>
* From a cluster node, run `ID=<db-id> /opt/recipe/snowflake/rs/run-test`.
* From a cluster node, run `/opt/recipe/snowflake/sample-snowsql-db`

## Diagnostics
<<TODO do we need this ??? can we point to a more general "diagnosing your recipe">>>
### Gear status

* Check the Redis DB log for errors: `/var/opt/redislabs/log/redis-*.log`

### Snowflake status

* Run `echo "select count(*) from person1;" | snowflake`
