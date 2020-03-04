# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on Ubuntu Xenial/Ubuntu Bionic/RHEL 7
* DataStax Cassandra cluster (via Docker)

## Install and configure Cassandra cluster

* Designate a machine with at least 10GB free disk space to host the Cassandra cluster.
  * Find the IP address of the machine and make sure port 9042 is open for inbound TCP traffic.
* [Install Docker](#insalling_docker).
* Install git.
* Setup Cassandra cluster and create a database:
```
bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-cql)
```
* It's now possible connect to the database using `/opt/recipe/cassandra/cqlsh`, and check that the tables were created (the tables are obviously empty):
```
select * from persons;
select * from cars;
```
## Installing Redis Gears on Redis Enterprise Cluster

* [Create a Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).
* On a cluster node, run (as root, via `sudo bash` for RHEL or `sudo su -` for Ubuntu):
```
CASSANDRA='<ip>' bash <(curl -fsSL https://cutt.ly/redisgears-wb-cql-node)
```

* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.
  * Add the following parameter: `CreateVenv 0`

## Running the write-behind recipe

On a Redis cluster node:

* Run `/opt/recipe/cassandra/start-write-behind`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/cassandra/start-write-behind`.

### Basic tests
If you created the example database, you can run the following tests to verify if your setup is working correctly.

* From within `bdb-cli <db-id>`, `RG.DUMPREGISTRATIONS` will return a list of registrations.
* Using `bdb-cli <db-id>`, invoke:
```
HSET person:007 first_name "James" last_name "Bond" age "42"
```
* Verify a record was created on Cassandra. From ```/opt/recipe/cassandra/cqlsh``` invoke:
```
select * from person;
```

## Testing
* Log on via SSH to a Redis cluster node.
* Run `/opt/recipe/rs/run-test`
* With multiple databases:
  * Inspect `rladmin status`
  * Run `DB=<db-id> /opt/recipe/rs/run-test`
* Open another connection to that node and run `/opt/recipe/cassandra/sample-cassandra-db`

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

* `redis-cli` via `bdb-cli <db-id>`
  * `RG.DUMPEXECUTIONS` command

### Cassandra status

* `/opt/recipe/cassandra/sample-cassandra-db` will repeatedly print number of records in the Cassandra test table.
* Cassandra CLI: `/opt/recipe/cassandra/cqlsh`
* Cassandra cluster information: `/opt/recipe/cassandra/nodetool`

## Appendixes

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

