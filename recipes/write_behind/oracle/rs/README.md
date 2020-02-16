# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on Ubuntu Xenial/Ubuntu Bionic/RHEL 7
* Oracle database (tested with 11g and 12c)
* RedisGears module for a matching platform

## Configuration

TBD: key names and tables

## Install and configure Oracle server

* Designate a machine with at least 10GB free disk space to host the Oracle database server.
  * Find the IP address of the machine and make sure port 1521 is open for inbound TCP traffic.
* [Install Docker](#insalling_docker).
* Install git.
* Setup Oracle server container and create a database:
```
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-oracle)
```
* It's now possible connect to the database using `/opt/recipe/oracle/sqlplus`, and check that the tables were created (the tables are obviously empty):
```
select * from person1;
select * from car;
```
## Installing Redis Gears on Redis Enterprise Cluster

* [Create a Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).
* On each cluster node, run (as root, via `sudo bash` for RHEL or `sudo su -` for Ubuntu):
```
ORACLE=<ip> bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-oracle-node)
```
* Download the Redis Gears module and add it to the cluster modules list.
	* For Ubuntu Xenial, use [this](http://redismodules.s3.amazonaws.com/redisgears/snapshots/redisgears.linux-xenial-x64.master.zip).
	* For Ubuntu Bionic, use [this](http://redismodules.s3.amazonaws.com/redisgears/snapshots/redisgears.linux-bionic-x64.master.zip).
	* For RHEL7, use [this](http://redismodules.s3.amazonaws.com/redisgears/snapshots/redisgears.linux-centos7-x64.master.zip).

* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.
  * Add the following parameter: `CreateVenv 0`

## Running the write-behind recipe

On one of the Redis cluster nodes:

* Run `/opt/recipe/rs/start-write-behind`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/rs/start-write-behind`.

### Basic tests
If you created the example database, you can run the following tests to verify if your setup is working correctly.

* From within `bdb-cli <db-id>`, `RG.DUMPREGISTRATIONS` will return a list of registrations.
* Using `bdb-cli <db-id>`, invoke:
```
HSET person2:johndoe first_name "John" last_name "Doe" age "42"
```
* Verify a record was created on Oracle. From ```/opt/recipe/oracle/sqlplus``` invoke:
```
select * from person1;
```

## Testing
* Log on via SSH to a cluster node.
* Run `/opt/recipe/rs/run-test`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/rs/run-test`.
* Open another connection to that node and run `/opt/recipe/oracle/sample-oracle-db`

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

### Oracle status

* `/opt/recipe/snowflake/sample-snowsql-db` will repeatedly print number of records in the Oracle test table.
* Oracle CLI: `sqlplus`

## Appendixes

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

