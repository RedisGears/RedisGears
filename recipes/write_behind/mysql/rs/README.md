# RedisGears Write-Behind Recipe

## System requirements

* Redis v5.0.7 or above
* Relational database: MySQL database
* RedisGears module built for Ubuntu Bionic
* Optional: Redis Enterprise Software v5.4.11-2 or above

## Configuration

TBD: key names and tables

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

#### Install and configure MySQL

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

#### Configure cluster nodes

* For each node, add the following to its `/etc/hosts` file, where `MYSQL-IP` is the controlling node IP:

```
MYSQL-IP mysql
```

* Install MySQL client with `/opt/gears-wb/mysql/install-mysql-client`.
* Install Oracle python client with `/opt/gears-wb/mysql/install-mysql-python-client`.

#### Run the Gear

* Run `/opt/gears-wb/mysql/rs/start-gear`.
* From within `redis-cli`, `RG.DUMPREGISTRATIONS` will return a list of registrations.

## Testing

* From the controlling node, run `/opt/gears-wb/test/test_write_behind.py`.

## Diagnostics

Gear status

MySQL status

MySQL status

## Appendix

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

