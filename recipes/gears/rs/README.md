# RedisGears on Redis Enterprise Software cluster

## System requirements

* Redis Enterprise Software v5.4.11-2 or above running on Ubuntu Xenial/Ubuntu Bionic/RHEL 7
* RedisGears module for a matching platform

## Configuration

TBD

## Installing Redis Gears on Redis Enterprise Cluster

* [Create a Redis Enterprise cluster](https://docs.redislabs.com/latest/rs/installing-upgrading/downloading-installing/).
* On each cluster node, run (as root, via `sudo bash` for RHEL or `sudo su -` for Ubuntu):
```
bash <(curl -fsSL https://cutt.ly/redisgears-setup-node)
```
* Download the Redis Gears module and add it to the cluster modules list.
	* For Ubuntu Xenial, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-xenial-x64.99.99.99-3e6d45a.zip).
	* For Ubuntu Bionic, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-bionic-x64.99.99.99-3e6d45a.zip).
	* For RHEL7, use [this](http://redismodules.s3.amazonaws.com/lab/08-gears-write-behind/redisgears.linux-centos7-x64.99.99.99-3e6d45a.zip).

* [Create a redis database](https://docs.redislabs.com/latest/modules/create-database-rs/) with RedisGears enabled.

## Testing

TBD

## Diagnostics

TBD

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
