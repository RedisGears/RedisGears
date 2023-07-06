---
title: "Development"
linkTitle: "Development"
weight: 3
description: >
  How to develop Triggers and Functions
---

When the project is small it is acceptable to have a single file that contains the entire code base. But as the project grows and becomes complex it is more difficult to maintain it as a single file project. Lucky for us, JS already addressed such problem and has the relevant tool to maintain a multi file project and compress it on build time to a single file that contains all the code. In this tutorial we will explain how to create a multi file project and how to wrap it as a single file and send it to Triggers and Functions. The tutorial assumes you have Redis with the RedisGears 2.0 module installed on `localhost:6379`. See [getting started](../README.md) section for installation instructions.

## Pre-requisite

* npm

## Creating the Project

Lets first create a directory to store our project and initialize our project with npm:
```bash
mkdir test
cd test
npm init -y
```

The above will generate a `package.json` file that looks as follows:

```json
{
  "name": "test",
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

In order to wrap our multi file project into a single file, we will use [webpack](https://webpack.js.org/). Let's install the required webpack tools:

```bash
npm install webpack webpack-cli --save-dev
```

Now let's create the `src` directory that will contain our code and inside it create a single file, `index.js`, that will be used as our main file.

```bash
mkdir src
cd src
touch index.js
```

Now let's add some code to `index.js`. Open `index.js` file and paste in the following code:

```js
redis.register_function("hello", function(){
    return "Hello World";
});
```

The next step is to create the webpack configuration file. On the project root directory, create a file called `webpack.config.js` and add the following code:

```js
const webpack = require('webpack');

module.exports = {
    entry: './src/index.js',
    mode: "production",
    plugins: [
        new webpack.BannerPlugin({
			banner:'#!js name=myFirstLibrary',
            raw: true,
            entryOnly: true,
	    })
	]
}
```

The `entry` field is the entry point of our project. The plugin we use instructs webpack to add a banner line at the beginning of the generated code that will contain the shebang syntax required by Triggers and Functions along side the library name.

We can now build our project, from within the root directory run the following command:

```bash
npx webpack --config webpack.config.js
```

If all was done correctly you will see a new directory, `dist`, with a single file, `main.js`, that contains the following code:

```js
#!js api_version=1.0 name=myFirstLibrary
redis.register_function("hello", (function() { return"Hello World" }));
```

This file can be sent to be evaluated by Triggers and Functions using `redis-cli`. From the project root directory, run the following:

```bash
redis-cli -x RG.FUNCTION LOAD < ./dist/main.js
```

An `OK` reply will indicating that the library was loaded successfully. Test the library functionality by running the following:

```bash
> redis-cli TFCALL myFirstLibrary.hello 0
"Hello World"
```

## Adding Files to our Project

Let's add another file under the `src` directory called `test.js` that contains the following code:

```js
export var test = 'test';
```

Modify `index.js` to import the `test` variable from `test.js`:

```js
import {test} from "./test.js"

redis.register_function("hello", function(){
    return test;
});
```

If we will compile our code again:

```bash
npx webpack --config webpack.config.js
```

we will see that the generated file content has changed and it now contains the following code:

```js
#!js api_version=1.0 name=myFirstLibrary
(()=>{"use strict";redis.register_function("hello",(function(){return"test"}))})();
```

Now we can upload our function. Notice that we use the UPGRADE option to upgrade the existing function; forgetting to do so will result in an error.

```bash
redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js
```

And we can test our function:

```bash
> redis-cli TFCALL myFirstLibrary.hello 0
"test"
```

## Using an External Library

Now let's try using an external library, for example `mathjs`. To install the library, run the following npm command on the project root directory:

```
npm install mathjs --save
```

Modify our program to use the `pi` variable imported from `mathjs` library:

```js
import {pi} from "mathjs"

redis.register_function("hello", function(){
    return pi;
});
```

Again, compile our project:

```bash
npx webpack --config webpack.config.js
```

Upgrade our library:

```bash
redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js
```

And run it:

```bash
> redis-cli TFCALL myFirstLibrary.hello 0
"3.1415926535897931"
```

Notice that Triggers and Functions **only supports pure JS libraries**. A library that has native code or uses some native JS APIs provided by the browser or by nodejs **will not work**.

## Easy Build and Deploy

We can use an npm scripts section to achieve easy to use build and deploy commands. Change the scripts section on `package.json` to the following:

```json
"scripts": {
    "build": "npx webpack --config webpack.config.js",
    "deploy": "echo \"Building\";npm run build;echo \"Deploying\";redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js"
}
```

Now we can run `npm run build` and `npm run deploy` to build and deploy our library to a local Redis server.

```bash
npm run deploy

> test@1.0.0 deploy
> echo "Building";npm run build;echo "Deploying";redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js

Building

> test@1.0.0 build
> npx webpack --config webpack.config.js

asset main.js 71.7 KiB [compared for emit] [minimized] (name: main) 1 related asset
orphan modules 1.88 MiB [orphan] 956 modules
runtime modules 248 bytes 3 modules
cacheable modules 1.4 MiB
  modules by path ./node_modules/ 254 KiB
    modules by path ./node_modules/seedrandom/ 26.5 KiB
      modules by path ./node_modules/seedrandom/lib/*.js 16 KiB 6 modules
      modules by path ./node_modules/seedrandom/*.js 10.5 KiB 2 modules
    ./node_modules/decimal.js/decimal.js 133 KiB [built] [code generated]
    ./node_modules/complex.js/complex.js 29.9 KiB [built] [code generated]
    ./node_modules/fraction.js/fraction.js 20.2 KiB [built] [code generated]
    ./node_modules/typed-function/typed-function.js 42.9 KiB [built] [code generated]
    ./node_modules/javascript-natural-sort/naturalSort.js 2.05 KiB [built] [code generated]
  ./src/index.js + 301 modules 1.15 MiB [built] [code generated]
  crypto (ignored) 15 bytes [optional] [built] [code generated]
webpack 5.73.0 compiled successfully in 3303 ms
Deploying
OK
```
