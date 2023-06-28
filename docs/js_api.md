# JS API

The following [directory](../redisgears_js_api) contains a node project that defined the entire API provided by RedisGears, it uses `.d.ts` files for auto complete and typing. You can install it using the following command:

```bash
npm install https://gitpkg.now.sh/RedisGears/RedisGears/redisgears_js_api
```

Or just add it as a dependency to you `package.json` file:

```json
"devDependencies": {
  "@redis/gears-api": "https://gitpkg.now.sh/RedisGears/RedisGears/redisgears_js_api"
}
```

Usage example:

```JS
#!js name=lib api_version=1.1

import { redis } from '@redis/gears-api';

redis.registerFunction('foo', (c)=>{
  return c.call('ping');
});
```


`redisgears_js_api` also contains a bundler that can be use to bundle you code and deploy it. Add the following to your `package.json` (assuming your main file is `index.js`):

```json
"scripts": {
  "deploy": "gears-api index.js"
},
```

Then run the following to deploy you code:

```bash
> npm run deploy

> deploy
> gears-api index.js

Deployed! :)
```
