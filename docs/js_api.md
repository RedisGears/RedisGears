# JS API

The following [directory](../redisgears_v8_plugin/js_api) contains a node project that defined the entire API provided by RedisGears, it uses `.d.ts` files for auto complete and typing. You can install it using the following command:

```bash
npm install https://gitpkg.now.sh/RedisGears/RedisGears/redisgears_v8_plugin/js_api
```

Usage example:

```JS
#!js name=lib api_version=1.1

import { redis } from '@redis/gears-api/gears-api';

redis.registerFunction('foo', (c)=>{
  return c.call('ping');
});
```
