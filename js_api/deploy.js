#!/usr/bin/env node

import { Command } from 'commander';
import { readFile } from 'fs/promises';
import { createClient } from '@redis/client';
import { resolve } from 'path';
import * as rollup from 'rollup';
import { preserveShebangs } from 'rollup-plugin-preserve-shebangs';
import nodeResolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

await new Command('deploy')
  .argument('<filename>')
  .option('-r, --redis [redis]')
  .option('-c, --config <config>')
  .option('-d, --debug')
  .option('-w, --watch')
  .action(async (filename, { redis, config, debug, watch }) => {
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
    }, config);
  })
  .parseAsync();

async function buildAndDeploy(client, rollupOptions, config) {
  try {
    await deploy(client, await rollup.rollup(rollupOptions), config);
  } finally {
    await client.quit();
  }
}

async function buildAndDeployAndDebug(client, rollupOptions, config) {
  try {
    await deploy(client, await rollup.rollup(rollupOptions), config, true);
  } finally {
    await client.quit();
  }
}

async function watchAndDeploy(client, rollupOptions, config) {
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
        return deploy(client, evt.result, config);
    }
  });
}

async function deploy(client, result, config, debug) {
  try {
    var args = ['TFUNCTION', 'LOAD', 'REPLACE']

    if (config){
      let configfile = ''
      try {
        JSON.parse(config)
        configfile = config
      } catch (error) {
        try {
          configfile =  await readFile(resolve(process.cwd(), config));
        } catch (error) {
          throw error
        } 
      }
      args.push('CONFIG');
      args.push(`${configfile.toString()}`)
    }

    if (debug === true) {
      const { output: [{ code }] } = await result.generate({ format: 'es', sourcemap: 'inline' });
      args.push(code)
      args.push('debug')
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