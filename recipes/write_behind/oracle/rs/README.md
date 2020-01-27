# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on Ubuntu Bionic or RHEL 7
* Oracle database (tested with 11g and 12c)
* RedisGears module built for Ubuntu Bionic or RHEL 7

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
## Installing the Redis cluster

* Create an un-bootstrapped Redis Enterprise cluster.
* On each cluster node, run:
```
ORACLE=<ip> bash <(curl -fsSL https://cutt.ly/redisgears-wb-setup-oracle-node)
```
* Bootstrap the cluster.

## Running the write-behind gear

On one of the Redis cluster nodes:

* Run `ID=<db-id> /opt/recipe/oracle/rs/start-gear`.

### Basic tests

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

* From a cluster node, run `ID=<db-id>/opt/recipe/oracle/rs/run-test`.
* Run `echo "select count(*) from person1;" | /opt/recipe/oracle/sqlplus`

## Diagnostics

### Gear status

* Check the Redis DB log for errors: `/var/opt/redislabs/log/redis-*.log`

### Oracle status

* Run `echo "select count(*) from person1;" | /opt/recipe/oracle/sqlplus`

## Appendixes

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

