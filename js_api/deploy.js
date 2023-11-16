#!/usr/bin/env node

import { Command } from 'commander';
import { createClient } from '@redis/client';
import { resolve } from 'path';
import * as rollup from 'rollup';
import { preserveShebangs } from 'rollup-plugin-preserve-shebangs';
import nodeResolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

await new Command('deploy')
  .argument('<filename>')
  .option('-r, --redis [redis]')
  .option('-d, --debug')
  .option('-w, --watch')
  .action(async (filename, { redis, debug, watch }) => {
    const client = createClient({ url: redis });
    client.on('error', err => console.error('Redis client error', err));
    await client.connect();

    var handler = watch ? watchAndDeploy : debug ? buildAndDeployAndDebug : buildAndDeploy;

    return handler(client, {
      input: {
        file: resolve(process.cwd(), filename)
      },
      plugins: [
        preserveShebangs(),
        nodeResolve(),
        commonjs(),
      ]
    });
  })
  .parseAsync();

async function buildAndDeploy(client, rollupOptions) {
  try {
    await deploy(client, await rollup.rollup(rollupOptions));
  } finally {
    await client.quit();
  }
}

async function buildAndDeployAndDebug(client, rollupOptions) {
  try {
    await deploy(client, await rollup.rollup(rollupOptions), true);
  } finally {
    await client.quit();
  }
}

async function watchAndDeploy(client, rollupOptions) {
  const watcher = await rollup.watch({
    ...rollupOptions,
    watch: {
      skipWrite: true
    }
  });

  watcher.on('event', evt => {
    switch (evt.code) {
      case 'ERROR':
        console.error('Error', evt);
        break;

      case 'BUNDLE_END':
        return deploy(client, evt.result);
    }
  });
}

async function deploy(client, result, debug) {
  try {
    var args = ['TFUNCTION', 'LOAD', 'REPLACE']
    if (debug === true) {
      args.push('debug')
      const { output: [{ code }] } = await result.generate({ format: 'es', sourcemap: 'inline' });
      args.push(code)
    } else {
      const { output: [{ code }] } = await result.generate({ format: 'es' });
      args.push(code)
    }
    var result = await client.sendCommand(args);
    if (debug === true) {
      console.log(result)
    } else {
      console.log('Deployed! :)');
    }
  } catch (err) {
    console.error('Deploy error', err);
  }
}


