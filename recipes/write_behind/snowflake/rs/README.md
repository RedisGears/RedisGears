# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on RHEL7
* Snowflake DB account
* RedisGears module built for RHEL7/CentOS7

## Configuration

TBD: key names and tables

## Installing the Redis cluster

* Create an un-bootstrapped Redis Enterprise cluster.
* On each cluster node, run:
```
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-node-snowflake)
```
* Configure Snowflake DB connection:

  * Run `sudo snowsql`. This will create the default configuration file in `/root/.snowsql/config`.

  * Configure Snowflake connection details in `/root/.snowsql/config`. This should be in a form similar to:

```
[connections]
accountname = "CODE.eu-west-1"
username = "USERNAME"
password = "PASSWORD"
```

* Bootstrap the cluster.

## Running the write-behind gear

On one of the Redis cluster nodes:

* Run `ID=<db-id> /opt/recipe/snowflake/rs/start-gear`.

### Basic tests

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

* From a cluster node, run `ID=<db-id> /opt/recipe/snowflake/rs/run-test`.
* Run `echo "select count(*) from person1;" | snowsql`

## Diagnostics

### Gear status

* Check the Redis DB log for errors: `/var/opt/redislabs/log/redis-*.log`

### Snowflake status

* Run `echo "select count(*) from person1;" | snowflake`

