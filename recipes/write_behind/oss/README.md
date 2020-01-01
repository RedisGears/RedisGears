# RedisGears Write-Behind Recipe

## System requirements

* Redis v5.0.7 or above
* Relational database: MySQL (any version will do) or Oracle (tested with 11g and 12c) database
* RedisGears module built for Ubuntu Bionic
* Optional: Redis Enterprise Software v5.4.11-2 or above

## Configuration

TBD: key names and tables

## Installing with OSS Redis

* Install [Docker](#insalling_docker) and git.
* Clone the RedisGears repo: `git clone github.com/RedisGears/RedisGears.git`
* Run:
```
docker run --name gears -d redisgears:latest
docker run --name mysql -d mysql:latest
```
TBD

## Testing

* From the controlling node, run `/opt/gears-wb/test/test_write_behind.py`.

## Diagnostics

Gear status

MySQL status

Oracle status

## Appendix

### Installing Docker {#installing_docker}
Run the following:
```
bash <(curl -fsSL https://get.docker.com)
systemctl enable docker
```

Verify with ```docker version```.

