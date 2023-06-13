# Create a Development Environment

When the project is small it is acceptable to have a single file that contains the entire code base. But as the project grows and become complex it is less comfortable to maintain it as a single file project. Lucky for us JS already face such problem and has the relevant tool to maintain a multi file project and compress it on build time to a single file that contains all the code. In this tutorial we will explain how to create a multi file project and how to wrap it as a single file and send it to RedisGears. The tutorial assume you have Redis with RedisGears 2.0 installed on `localhost:6379`. See [getting started](../README.md) section for installation instructions.

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
redis.registerFunction("foo", function(){
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
			banner:'#!js name=foo api_version=1.0',
            raw: true,
            entryOnly: true,
	    })
	]
}
```

The `entry` field is the entry point of our project. The plugin we use instruct webpack to add a banner line at the beginning of the generated code that will contains the shebang syntax required by RedisGears along side the library name.

We can now build our project, from within the root directory run the following command:

```bash
npx webpack --config webpack.config.js
```

If all was done correctly you will see a new directory, `dist`, with a single file, `main.js`, that contains the following code:

```js
#!js api_version=1.0 name=foo
redis.registerFunction("foo", (function() { return"foo" }));
```

This file can be send to be evaluated by RedisGears using `redis-cli`. From the project root directory, run the following:

```bash
redis-cli -x TFUNCTION LOAD < ./dist/main.js
```

An `OK` reply will indicating that the library was loaded successfully. Test the library functionality by running the following:

```bash
> redis-cli TFCALL foo.foo 0
"foo"
```

## Adding Files to our Project

Lets adda another file under the `src` directory called `test.js` that contains the following code:

```js
export var test = 'test';
```

Lets modify `index.js` to import the `test` variable from `test.js`:

```js
import {test} from "./test.js"

redis.registerFunction("foo", function(){
    return test;
});
```

If we will compile our code again:

```bash
npx webpack --config webpack.config.js
```

We will see that the generated file content has changed and it is now contains the following code:

```js
#!js api_version=1.0 name=foo
(()=>{"use strict";redis.registerFunction("foo",(function(){return"test"}))})();
```

Now we can upload our function (notice that we use the `REPLACE` option to replace the existing function):

```bash
redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js
```

And we can test our function:

```bash
> redis-cli TFCALL foo.foo 0
"test"
```

## Using an External Library

Now lets use some external library, for example `mathjs`. To install the library run the following npm command on the project root directory:

```
npm install mathjs --save
```

Lets change our program to use `pi` variable imported from `mathjs` library:

```js
import {pi} from "mathjs"

redis.registerFunction("foo", function(){
    return pi;
});
```

Again lets compile our project:

```bash
npx webpack --config webpack.config.js
```

REPLACE our library:

```bash
redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js
```

And run it:

```bash
> redis-cli TFCALL foo.foo 0
"3.1415926535897931"
```

Notice that RedisGears **only supports pure JS libraries**, a library that has a native code or use some native JS API provided by the browser or by nodejs **will not work**.

## Easy Build and Deploy

We can use npm scripts section to achieve an easy build and deploy commands, change the scripts section on `package.json` to the following:

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

# Using Webassembly

**The following tutorial is experimental and is not promised to be supported in the final GA version**

Fortunately for us, v8 comes with an embedded webassembly support. We can leverage it to run our code that was written in some low level language, and achieve better performance on CPU intensive tasks. In this tutorial we will show how to compile and run a rust code inside v8 webassembly embedded inside Redis using RedisGears :).

## Pre-requisite

* rust
* npm

## Write and Build the Rust Code

First let install [wasm-pack]():

```
cargo install wasm-pack
```

Now we need to initiate our project, we will use [rust pack](https://github.com/rustwasm/rust-webpack-template):

```bash
> npm init rust-webpack redisgears_rust
 🦀 Rust + 🕸 WebAssembly + Webpack = ❤️
 Installed dependencies ✅
```

`cd` in `redisgears_rust` directory. You will notice we have `src` directory with a single file inside it, `lib.rs`. Let open this file and add the following content:

```rust
extern crate wasm_bindgen;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern {
    #[wasm_bindgen(js_namespace = redis)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn test() {
    log("Hello, World!");
}
```

The code defines a `test` function that simply prints `Hello, World!`. Notice that it uses the `log` function which defined as an extern function, this is because it is implemented on the `JS` under namespace `redis` (simply the `redis.log` function). Now we can simply build our project:

```bash
wasm-pack build --target web
```

Notice that we are building using the `web` target, though we are not actually a web, this target will be good enough for us and will help to avoid `nodejs` specific API that are not supported inside RedisGears (we hope that one day we will be popular enough and a new `RedisGears` target will be added :)).

After build successfully, you will notice that a new `pkg` directory was created containing the following content:

```bash
package.json
README.md
rust_webpack_template_bg.wasm
rust_webpack_template_bg.wasm.d.ts
rust_webpack_template.d.ts
rust_webpack_template.js
```

We will only use those 2 files:

```
rust_webpack_template_bg.wasm
rust_webpack_template.js
```

For convenience, let add this build processes to be part of our `npm run deploy` command (as describe on [Easy Build and Deploy](#easy-build-and-deploy)), the scripts section will look like this (assuming you created `redisgears_rust` project under the root directory on the main project):

```json
"scripts": {
    "build_r":"cd redisgears_rust; wasm-pack build --target web",
    "build":  "npm run build_r; npx webpack --config webpack.config.js",
    "deploy": "echo \"Building\";npm run build;echo \"Deploying\";redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js"
  },
```

## Bundle Our Compiled Code With Webpack

Now we will need to create a single bundled file that contain it all (`JS` and `webassembly`). We will generate a `JS` code that contains the webassembly code as base64 encoded string. On runtime, we will decode the string and run our webassembly code. Before we modify our `src/index.js` code to do that, let install some needed libraries:

```bash
npm install base-64 modify-source-webpack-plugin text-encoding --save
```

Now lets modify `src/index.js` to decode our base64 string and load the webassembly module. Then we will call `test` function that was exported by our rust code.

```js
import wasm_base_64 from "../redisgears_rust/pkg/rust_webpack_template_bg.wasm";
import {test, initSync} from "../redisgears_rust/pkg/rust_webpack_template.js";
import base64 from 'base-64';

var data = wasm_base_64.split(",")[1];
var wasm_code = base64.decode(data);

var decoded_wasm = Uint8Array.from(wasm_code, c => c.charCodeAt(0));
initSync(decoded_wasm)

redis.registerFunction("foo", function(){
    return test();
});

```

`wasm_base_64` argument will contains our webassembly code encoded as base64 (we will show later how to manipulate `webpack.config.js` file to achieve that). The second line imports `test` and `initSync` function. We will call `initSync` to initiate our webassembly module (after decoding it). Then we can use `test` function wherever we want.

Now we need to modify `webpack.config.js` to the following:

```js
const webpack = require('webpack');
const { ModifySourcePlugin } = require('modify-source-webpack-plugin');

module.exports = {
    mode: 'production',
    entry: './src/index.js',
    plugins: [
        new webpack.BannerPlugin({
          banner:'#!js name=foo api_version=1.0',
                raw: true,
                entryOnly: true,
        }),
        new webpack.ProvidePlugin({
            TextDecoder: ['text-encoding', 'TextDecoder'],
            TextEncoder: ['text-encoding', 'TextEncoder']
        }),
        new ModifySourcePlugin({
            rules: [
                {
                    test: /rust_webpack_template\.js$/,
                    modify: (src) => {
                        return src.replace("import.meta.url", "");
                    }
                }
            ]
        })
	  ],
    module: {
        rules: [
            {
                test: /\.wasm/,
                type: 'asset/inline'
            }
        ]
    },
}

```

Lets go through the changes:

```js
new webpack.ProvidePlugin({
            TextDecoder: ['text-encoding', 'TextDecoder'],
            TextEncoder: ['text-encoding', 'TextEncoder']
        }),
```

This was added so that webpack will add `TextDecoder` and `TextEncoder` to the final `JS` code, those are used by the `rust_webpack_template.js`.


```js
module: {
        rules: [
            {
                test: /\.wasm/,
                type: 'asset/inline'
            }
        ]
    },
```

This tells webpack to embed any imported `wasm` file as using base64 encoding.

```js
new ModifySourcePlugin({
            rules: [
                {
                    test: /rust_webpack_template\.js$/,
                    modify: (src) => {
                        return src.replace("import.meta.url", "");
                    }
                }
            ]
        })
```

This tells webpack to replace all `import.meta.url` with an empty string. We need it because when webpack sees `import.meta.url` it replace it with some `JS` code that uses `document` object which we do not have on RedisGears.

And finally we are done, we can build and deploy our code:

```bash
> npm run deploy

> redisgears_rust@1.0.0 deploy
> echo "Building";npm run build;echo "Deploying";redis-cli -x TFUNCTION LOAD REPLACE < ./dist/main.js

Building

> redisgears_rust@1.0.0 build
> npm run build_r; npx webpack --config webpack.config.js


> redisgears_rust@1.0.0 build_r
> cd redisgears_rust; wasm-pack build --target web

[INFO]: Checking for the Wasm target...
[INFO]: Compiling to Wasm...
warning: Found `debug_assertions` in `target.'cfg(...)'.dependencies`. This value is not supported for selecting dependencies and will not work as expected. To learn more visit https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#platform-specific-dependencies
    Finished release [optimized] target(s) in 0.01s
[INFO]: Installing wasm-bindgen...
[INFO]: Optimizing wasm binaries with `wasm-opt`...
[INFO]: Optional fields missing from Cargo.toml: 'repository', 'license'. These are not necessary, but recommended
[INFO]: :-) Done in 0.16s
[INFO]: :-) Your wasm pkg is ready to publish at /redisgears_rust/redisgears_rust/pkg.
asset main.js 539 KiB [compared for emit] [minimized] [big] (name: main) 1 related asset
orphan modules 3.14 KiB [orphan] 2 modules
runtime modules 344 bytes 2 modules
cacheable modules 625 KiB
  modules by path ./node_modules/text-encoding/ 617 KiB
    ./node_modules/text-encoding/index.js 258 bytes [built] [code generated]
    ./node_modules/text-encoding/lib/encoding.js 99.1 KiB [built] [code generated]
    ./node_modules/text-encoding/lib/encoding-indexes.js 518 KiB [built] [code generated]
  ./src/index.js + 2 modules 3.56 KiB [built] [code generated]
  ./node_modules/base-64/base64.js 4.78 KiB [built] [code generated]

WARNING in asset size limit: The following asset(s) exceed the recommended size limit (244 KiB).
This can impact web performance.
Assets:
  main.js (539 KiB)

WARNING in entrypoint size limit: The following entrypoint(s) combined asset size exceeds the recommended limit (244 KiB). This can impact web performance.
Entrypoints:
  main (539 KiB)
      main.js


WARNING in webpack performance recommendations:
You can limit the size of your bundles by using import() or require.ensure to lazy load some parts of your application.
For more info visit https://webpack.js.org/guides/code-splitting/

webpack 5.74.0 compiled with 3 warnings in 1292 ms
Deploying
OK
```

Now if we will call `foo` function using [`TFCALL`](commands.md#tfcall), we will see that `Hello, World!` is printed to the Redis log file.

```bash
> redis-cli TFCALL foo.foo 0
"undefined"
```

On Redis log file:

```bash
159121:M 01 Aug 2022 17:20:53.984 * <redisgears_2> Hello, World!
```

## Exposing JS API to the Rust Code

Lets assume that we want to expose `GET` command to the rust code. We can easily do it by defining a client object on the `JS` code and pass it to the rust code.

Lets change `./src/index.js` to the following:

```js
import wasm_base_64 from "../redisgears_rust/pkg/rust_webpack_template_bg.wasm";
import {test, initSync} from "../redisgears_rust/pkg/rust_webpack_template.js";
import base64 from 'base-64';

var data = wasm_base_64.split(",")[1];
var wasm_code = base64.decode(data);

var decoded_wasm = Uint8Array.from(wasm_code, c => c.charCodeAt(0));
initSync(decoded_wasm)

redis.registerFunction("foo", function(client){
    var rust_client = new Object();
    rust_client.get = (key) => {
        return client.call("get", key);
    };
    return test(rust_client);
});

```

Notice that we are creating a `rust_client` object and we pass it the `test` function. Lets modify our rust code to accept this `rust_client`:

```rust
extern crate wasm_bindgen;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern {
    #[wasm_bindgen(js_namespace = redis)]
    fn log(s: &str);

    pub type Client;

    #[wasm_bindgen(method)]
    fn get(this: &Client, key: &str) -> Option<String>;
}

#[wasm_bindgen]
pub fn test(client: &Client) -> Option<String> {
    log("Hello, World!");
    client.get("x")
}

```

We define a new `Client` type and declare its `get` function. Then we use it on our `test` function and fetch the value of `x`. We can now recompile and deploy and if everything done correctly we can see that our rust code fetches `x` and returns its value:

```bash
> redis-cli
127.0.0.1:6379> TFCALL foo.foo 0
"undefined"
127.0.0.1:6379> set x 1
OK
127.0.0.1:6379> TFCALL foo.foo 0
"1"
```

You can now expose whatever API you want to the rust code and write rust code the way you like.

**You are welcome to come up with a new and nice ideas of improving the development environment and share it with us.**
