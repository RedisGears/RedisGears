# RedisGears Write-Behind Recipe

## System requirements

* Redis v5.0.7 or above
* Relational database: MySQL database
* RedisGears module built for Ubuntu Bionic
* Optional: Redis Enterprise Software v5.4.11-2 or above

## Configuration

TBD: key names and tables

### Install and configure MySQL

* Run `/opt/gears-wb/mysql/install-mysql-docker`. This will run an MySQL database in a container.
** Alternatively, run `/opt/gears-wb/mysql/install-mysql` to install MySQL directly on host.

* On the controlling node, install Oracle client with `/opt/gears-wb/mysql/install-mysql-client`.

* Add the following to `/etc/hosts`:

```
127.0.0.1 mysql
```

* Create a database with `/opt/gears-wb/mysql/rs/create-db`.

* It's now possible connect to the database using `mysql test/passwd@/localhost` and check that the tables were created:

```
select * from person1;
select * from car;
```

## Installing on Redis Enterprise Software cluster

* Create an un-bootstrapped Redis cluster.
* Install RedisGears on all cluster nodes with:
```
bash <(curl -fsSL http://tiny.cc/redisgears-wb-setup)
```
* Bootstrap the cluster.
* Download and extract the RedisGears Write-Behind Recipe archive into `/opt/gears-wb` on one of the nodes, which we'll refer to as the "**Controlling node**".

Depending on your favorite database, continue with either **Running with MySQL** or **Running with Oracle**.

### Running with MySQL

#### Configure cluster nodes

* For each node, add the following to its `/etc/hosts` file, where `MYSQL-IP` is the controlling node IP:

```
MYSQL-IP mysql
```

* Install MySQL client with `/opt/gears-wb/mysql/install-mysql-client`.
* Install Oracle python client with `/opt/gears-wb/mysql/install-mysql-python-client`.

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
* Verify a record was created on Oracle. From ```mysql``` invoke:
```
select * from person1;
```

## Testing
* Log on via SSH to a cluster node.
* Run `/opt/recipe/rs/run-test`.
* With multiple databases:
  * Inspect `rladmin status`,
  * Run `DB=<db-id> /opt/recipe/rs/run-test`.
* Open another connection to that node and run `/opt/recipe/oracle/sample-db`

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
  
### MySQL status

TBD

## Appendixes

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

