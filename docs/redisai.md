# RedisAI Integration

[RedisAI](https://oss.redis.com/redisai/) RedisAI is a Redis module for executing Deep Learning/Machine Learning models and managing their data. Its purpose is being a "workhorse" for model serving, by providing out-of-the-box support for popular DL/ML frameworks and unparalleled performance.
RedisGears has a built-in integration with RedisAI via Python plugin that enables the registration of AI flows, and triggering it upon events.

### Setup
To use RedisAI functionality with RedisGears, RedisAI module should be loaded to the Redis serve along with RedisGears.
The quickest way to try RedisGears with RedisAI is by launching `redismod` Docker container image that bundles together the latest stable releases of Redis and select Redis modules from Redis:

```docker run -p 6379:6379 redislabs/redismod:latest```

Alternatively, you can build RedisAI from its source code by following the instruction [here](https://oss.redis.com/redisai/quickstart/).
Then, you can run the following command to load the two modules (from RedisGears root directory):

```redis-server --loadmodule ./redisgears.so Plugin gears_pyhton.so --loadmodule <path/to/RedisAI-repo>/install-cpu/redisai.so```