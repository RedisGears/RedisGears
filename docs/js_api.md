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

## Full example:

Create an empty directory, `redisgears_project`. Inside the directory run the following command:

```bash
> npm init -y -f
npm WARN using --force Recommended protections disabled.
Wrote to /home/meir/work/RedisGearsJSSDK/gears_project_2/package.json:

{
  "name": "gears_project_2",
  "version": "1.0.0",
  "description": "",
  "main": "index.js",
  "scripts": {
    "test": "echo \"Error: no test specified\" && exit 1"
  },
  "keywords": [],
  "author": "",
  "license": "ISC"
}
```

If all went well you should see a `package.json` file, add the following content to `package.json`:

```js
{
  "name": "gears_project",
  "version": "1.0.0",
  "description": "",
  "main": "index.js",
  "scripts": {
    "deploy": "gears-api index.js"
  },
  "devDependencies": {
    "@redis/gears-api": "https://gitpkg.now.sh/RedisGears/RedisGears/redisgears_js_api"
  },
  "keywords": [],
  "author": "",
  "license": "ISC"
}
```

Install the dependencies using `npm install`.

Create a new file, `index.js` and add to following content to it:

```js
#!js name=lib api_version=1.0

import { redis } from '@redis/gears-api';

redis.registerFunction("test", ()=>{
    return "test";
});
```

Now you can deploy your code to RedisGears using the following command:

```bash
> npm run deploy -- -r redis://localhost:6379

> deploy
> gears-api index.js

Deployed! :)
```

The provided URL should follow the following format: `<redis[s]://[[username][:password]@][host][:port][/db-number]>`

If all went well you can now run your RedisGears function:

```bash
127.0.0.1:6379> TFCALL lib.test 0
"test"
```
