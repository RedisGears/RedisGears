# Create a Development Environemt

When the project is small it is accaptable to have a single file that contains the entire code base. But as the project grows and become complex it is less comftable to mantain it as a single file project. Lucky for us JS already face such problem and has the relevant tool to maintain a multi file project and compress it on build time to a single file that contains all the code. In this taturial we will explain how to create a multi file project and how to wrap it as a single file and send it to RedisGears. The taturial assume you have Redis with RedisGears 2.0 installed on `localhost:6379`. See [getting started](../README.md) section for installation instructions.

## Pre-requisite

* npm

## Creating the Project

Lets first create a directory to store our project and initialize our project with npm:
```bash
mkdir test
cd test
npm init -y
```

The above will generate a `package.json` file that looks as follow:

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

In order to wrap our multi file project into a single file, we will use [webpack](https://webpack.js.org/), let install the required webpack tools:

```bash
npm install webpack webpack-cli --save-dev
```

Now lets create the `src` directory that will contain our code. Inside it lets create a single file, `index.js`, that will be used as our main file.

```bash
mkdir src
cd src
touch index.js
```

Now lets add some code to `index.js`, open `index.js` file and past the following code:

```js
redis.register_function("foo", function(){
    return "foo";
});
```

The next step is to create the webpack configuration file. On the project root directory, generate a file called `webpack.config.js` and add the following code:

```js
const webpack = require('webpack');

module.exports = {
    entry: './src/index.js',
    mode: "production",
    plugins: [
        new webpack.BannerPlugin({
			banner:'#!js name=foo',
            raw: true,
            entryOnly: true,
	    })
	]
}
```

The `entry` field is the entery point of our project. The plugin we use instruct webpack to add a banner line at the begining of the generated code that will contains the shabeng syntax required by RedisGears along side the library name.

We can now build our project, from within the root directory run the following command:

```bash
npx webpack --config webpack.config.js
```

If all was done correctly you will see a new directiry, `dist`, with a single file, `main.js`, that contains the following code:

```js
#!js name=foo
redis.register_function("foo",(function(){return"foo"}));
```

This file can be send to be evaluated by RedisGears using `redis-cli`. From the project root directory, run the following:

```bash
redis-cli -x RG.FUNCTION LOAD < ./dist/main.js
```

An `OK` reply will indicating that the library was loaded successfully. Test the library functionality by running the following:

```bash
> redis-cli RG.FUNCTION CALL foo foo
"foo"
```

## Adding Files to our Project

Lets adda another file under the `src` direcotry called `test.js` that contains the following code:

```js
export var test = 'test';
```

Lets modify `index.js` to import the `test` variable from `test.js`:

```js
import {test} from "./test.js"

redis.register_function("foo", function(){
    return test;
});
```

If we will compile our code again:

```bash
npx webpack --config webpack.config.js
```

We will see that the generated file content has changed and it is now contains the following code:

```js
#!js name=foo
(()=>{"use strict";redis.register_function("foo",(function(){return"test"}))})();
```

Now we can upload our function (notice that we use the UPGRADE option to upgrade the existing function):

```bash
redis-cli -x RG.FUNCTION LOAD UPGRADE < ./dist/main.js
```

And we can test our function:

```bash
> redis-cli RG.FUNCTION CALL foo foo
"test"
```

## Using an External Library

Now lets use some exteral library, for example `mathjs`. To install the library run the following npm command on the project root directory:

```
npm install mathjs
```

Lets change our program to use `pi` variable imported from `mathjs` library:

```js
import {pi} from "mathjs"

redis.register_function("foo", function(){
    return pi;
});
```

Again lets compile our project:

```bash
npx webpack --config webpack.config.js
```

Upgrade our library:

```bash
redis-cli -x RG.FUNCTION LOAD UPGRADE < ./dist/main.js
```

And run it:

```bash
> redis-cli RG.FUNCTION CALL foo foo
"3.1415926535897931"
```

Notice that RedisGears **only supports pure JS libraries**, a library that has a native code or use some native JS API provided by the browser or by nodejs **will not work**.

## Easy Build and Deploy

We can use npm scripts section to achieve an easy build and deploy commands, change the scripts section on `package.json` to the following:

```json
"scripts": {
    "build": "npx webpack --config webpack.config.js",
    "deploy": "echo \"Building\";npm run build;echo \"Deploying\";redis-cli -x RG.FUNCTION LOAD UPGRADE < ./dist/main.js"
}
```

Now we can run `npm run build` and `npm run deploy` to build and deploy our library to a local Redis server.

```bash
npm run deploy

> test@1.0.0 deploy
> echo "Building";npm run build;echo "Deploying";redis-cli -x RG.FUNCTION LOAD UPGRADE < ./dist/main.js

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

You are welcome to come up with a new and nice ideas of impoving the development environment and share it with us.
