# RedisGears Write-Behind Recipe on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above
* Oracle database (tested with 11g and 12c)
* RedisGears module built for Ubuntu Bionic

## Configuration

TBD: key names and tables

## Install and configure Oracle

* Designate a machine with at least 20GB of free disk space to host the Oracle database.
  * Find the IP address of the machine and make sure port 1521 is open for inbound TCP traffic.
* [Install Docker](#insalling_docker).
* Clone the [RegisGears](https://github.com/RegisGears/RedisGears) repository:
```
mkdir -p /opt
cd /opt
git clone --branch rafi_behind-oracle-1 --single-branch https://github.com/RegisGears/RedisGears.git
```
* Create symlinks to the recipe directory for easier access:
```
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd recipe
ln -s ../gear.py
```
* Run `/opt/repice/oracle/install-oracle-docker`. This will run an Oracle database in a container.
* Run the following:
```
echo "127.0.0.1 oracle" >> /etc/hosts
```

* Create a database with `/opt/recipe/oracle/rs/create-db`.

* It's now possible connect to the database using:
```
. /etc/profile.d/oracle.sh
rlwrap sqlplus test/passwd@//localhost/xe
```
* And check that the tables were created (the tables are obviously empty):

```
select * from person1;
select * from car;
```
## Installing the Redis cluster

* Create an un-bootstrapped Redis Enterprise cluster.
* Install RedisGears on all cluster nodes with:
```
bash <(curl -fsSL http://tiny.cc/redisgears-wb-setup)
```
* Bootstrap the cluster.

* Find out Redis DB port and password.

  * You can verify those with: `redis-cli -p PORT -a PASSWORD`

## Configure cluster nodes
For each cluster node:

* Clone the [RegisGears](https://github.com/RegisGears/RedisGears) repository:
```
mkdir -p /opt
cd /opt
git clone --branch rafi_behind-oracle-1 --single-branch https://github.com/RegisGears/RedisGears.git
```
* Create symlinks to the recipe directory for easier access:
```
ln -s /opt/RedisGears/recipes/write_behind /opt/recipe
cd recipe
ln -s ../gear.py
```
* Install Oracle client with `/opt/recipe/oracle/install-oracle-client`.

* For each node, add the following to its `/etc/hosts` file, where `ORACLE-IP` is the controlling node IP:

```
ORACLE-IP oracle
```

* Install Oracle client with `/opt/recipe/oracle/install-oracle-client`.
* Install Oracle python client with `/opt/recipe/oracle/install-oracle-python-client`.

## Running the write-behind gear

From one of the Redis cluster nodes:

* Run `PORT=<n> PASSWORD=<passwd> /opt/recipe/oracle/rs/start-gear`.
* From within `redis-cli`, `RG.DUMPREGISTRATIONS` will return a list of registrations.
* Using ``redis-cli```, invoke:
```
HSET person2:johndoe first_name "John" last_name "Doe" age "42"
```
* Verify a record was created on Oracle. From ```/opt/recipe/oracle/sqlplus``` invoke:
```
select * from person1;
```

## Testing

* From the controlling node, run `/opt/recipe/test/test_write_behind.py`.

## Diagnostics

Gear status

MySQL status

Oracle status

## Appendixes

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

