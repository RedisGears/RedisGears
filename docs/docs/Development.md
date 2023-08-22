---
title: "Development"
linkTitle: "Development"
weight: 3
description: >
  How to develop for triggers and functions
---

To aid in the development of new libraries of triggers and functions you can use the type declaration files for the triggers and functions API. This can be used by your preferred development environment to provide autocompletion and type checking based on the type information. You can install it using the following command:

```bash
npm install https://gitpkg.now.sh/RedisGears/RedisGears/redisgears_js_api
```

Or add it directly as a dependency to your `package.json`:

```json
"devDependencies": {
  "@redis/gears-api": "https://gitpkg.now.sh/RedisGears/RedisGears/js_api"
}
```

## Example project setup

Create an empty directory for your new triggers and functions project, `my_first_project`. Navigate to the folder and run the following command:

```bash
npm init -y -f
npm WARN using --force Recommended protections disabled.
Wrote to /home/work/my_first_project/package.json:

{
  "name": "my_first_project",
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

Update the the `package.json` to add the devDependency to the gears API.

```js
{
  "name": "my_first_project",
  "version": "1.0.0",
  "description": "",
  "main": "index.js",
  "scripts": {
    "test": "echo \"Error: no test specified\" && exit 1"
  },
  "devDependencies": {
    "@redis/gears-api": "https://gitpkg.now.sh/RedisGears/RedisGears/js_api"
  },
  "keywords": [],
  "author": "",
  "license": "ISC"
}
```

Install the dependencies using `npm install`.

Create a new file, `index.js` and import the gears-api module. This will enable code intellisense when adding the `registerFunction`:

```JavaScript
#!js name=lib api_version=1.0

import { redis } from '@redis/gears-api';

redis.registerFunction("test", ()=>{
    return "test";
});
```

To automate the deployment, update the `scripts` section in `pacakage.json`:

```json
"scripts": {
  "deploy": "gears-api index.js"
}
```

Now you can deploy your code to RedisGears using the following command:

```bash
> npm run deploy -- -r redis://localhost:6379

> deploy
> gears-api index.js

Deployed! :)
```

The provided URL should follow the following format: `<redis[s]://[[username][:password]@][host][:port][/db-number]>`

You can now run your RedisGears function:

```bash
127.0.0.1:6379> TFCALL lib.test 0
"test"
```