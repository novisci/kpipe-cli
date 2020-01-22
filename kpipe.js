#!/usr/bin/env node

var wtf = require('wtfnode')

// Ensure that the .env file is read from the installation directory, not the current dir
const prjDir = __dirname.replace(/\/\w+$/, '/.env')
require('dotenv').config({
  path: prjDir
})

const cli = require('yargs')
  .option('debug', {
    describe: 'Enable debugging log output',
    type: 'boolean'
  })
  .option('verbose', {
    alias: 'v',
    describe: 'Enable verbose log output',
    type: 'boolean'
  })
  .option('trace', {
    describe: 'Emit a trace log (csv) of heap size over time'
  })

  .middleware((argv) => {
    require('./logger')({
      verbose: argv.verbose || argv.debug,
      debug: argv.debug
    })

    wtf.setLogger('info', console.info)
    wtf.setLogger('warn', console.error)
    wtf.setLogger('error', console.error)
  })
  .commandDir('cmds')
  .demandCommand()
  .help()
  .wrap(80)
  .argv
