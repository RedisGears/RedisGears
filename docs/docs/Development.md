---
title: "Development"
linkTitle: "Development"
weight: 4
description: >
  How to develop for triggers and functions
---

To aid in the development of new libraries of triggers and functions, you can use the type declaration files for the [triggers and functions API](/docs/interact/programmability/triggers-and-functions/concepts/javascript_api/), which allows your preferred development environment to provide autocompletion and type checking. You can install this information using the following command:

```bash
npm install https://gitpkg.now.sh/RedisGears/RedisGears/js_api
```

Or you can add it as a devDependency to your `package.json` file:

```json
"devDependencies": {
  "@redis/gears-api": "https://gitpkg.now.sh/RedisGears/RedisGears/js_api"
}
```

## Example project setup

Create an empty directory for your new triggers and functions project, `my_first_project`. Navigate to the folder and run the following command:

```bash
$ npm init -y -f
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

Update the `package.json` to add the gears API to the `devDependencies` object.

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

Create a new file, `index.js` and import the gears-api module. While this step is not strictly necessary, doing so will enable code intelligent code completion when adding the `registerFunction`:

```JavaScript
#!js name=lib api_version=1.0

import { redis } from '@redis/gears-api';

redis.registerFunction("test", ()=>{
    return "test";
});
```

To automate the deployment, update the `scripts` section in `package.json`:

```json
"scripts": {
  "deploy": "gears-api index.js"
}
```

Now you can deploy your code to Redis Stack using the following command:

```bash
> npm run deploy -- -r redis://localhost:6379

> deploy
> gears-api index.js

Deployed! :)
```

The provided URL should follow the following format:

`<redis[s]://[[username][:password]@][host][:port][/db-number]>`

You can now run your Redis Stack function:

```bash
127.0.0.1:6379> TFCALL lib.test 0
"test"
```

## Example function using a third-party `npm` package

If you want to use a third-party `npm` package in your Redis Stack function, the overall process is nearly the same as already discussed, with just a couple of differences.

First, you'll need to install your package using `npm install`. For this example, the `pi` symbol from the `math.js` library will be used in a simple function that calculates the area of a circle.

```bash
npm install mathjs
```

Note: your `package.json` file will also be updated with the new dependency. For example:

```json
"dependencies": {
    "mathjs": "^12.0.0"
}
```

Next, you'll import the required symbol(s) into your function.

```javascript
#!js api_version=1.0 name=lib

import { redis } from '@redis/gears-api';
import { pi } from 'mathjs';

function calculateCircleArea(client, radius) {
  return pi * radius * radius;
}

redis.registerFunction('calculateArea', calculateCircleArea);
```

Load and run your new function as described in the previous section.