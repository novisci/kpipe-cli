/***
 *
 */
const { PassThrough } = require('stream')
const { PipelinePromise } = require('kpipe-sequence')
const { Transform } = require('kpipe-streams')
const { Reader } = require('kpipe-core')

function readPipe (stream, argv = {}) {
  console.info('READ BEGIN')
  return require('../cli-cmd')(PipelinePromise(
    ...stream,
    require('kpipe-core').Writer({ type: 'stdio' })()
  )
    .catch((err) => console.error(err)), argv)
}

function decompressStream (argv) {
  if (argv.gunzip) {
    return Transform.Gunzip()
  } else if (argv.snappy) {
    return Transform.SnappyUncompress()
  } else {
    return new PassThrough()
  }
}

module.exports = {
  command: 'read <source>',
  desc: 'Read from stream to stdout',

  builder:
    (yargs) => yargs
      .demandCommand()
      .option('gunzip', {
        alias: 'z',
        type: 'boolean',
        describe: 'Decompress using gzip'
      })
      .option('snappy', {
        type: 'boolean',
        describe: 'Uncompress using snappy'
      })

      // stdin -> stdout
      .command({
        command: 'stdio',
        desc: 'Read from stdin to stdout',
        builder:
          (yargs) => yargs,
        handler:
          (argv) => readPipe([
            Reader({ type: 'stdio' })(),
            decompressStream(argv)
          ],
          argv)
      })

      // fs -> stdout
      .command({
        command: 'fs <filename>',
        desc: 'Read from file system to stdout',
        builder:
          (yargs) => yargs
            .positional('filename', {
              describe: 'Path to file',
              type: 'string'
            }),
        handler:
          (argv) => readPipe([
            Reader({ type: 'fs' })(argv.filename),
            decompressStream(argv)
          ],
          argv)
      })

      // s3 -> stdout
      .command({
        command: 's3 <region> <bucket> <path>',
        desc: 'Read from S3 to stdout',
        builder:
          (yargs) => yargs
            .positional('region', {
              describe: 'S3 region'
            })
            .positional('bucket', {
              describe: 'S3 bucket'
            })
            .positional('path', {
              describe: 'S3 object path'
            }),
        handler:
          (argv) => readPipe([
            Reader({
              type: 's3',
              region: argv.region,
              bucket: argv.bucket
            })(argv.path),
            decompressStream(argv)
          ],
          argv)
      })

      // random -> stdout
      .command({
        command: 'random <length>',
        desc: 'Read random rows to stdout',
        builder:
          (yargs) => yargs
            .positional('length', {
              describe: 'Number of lines to generate'
            })
            .option('width', {
              alias: 'w',
              describe: 'Width of rows',
              type: 'numeric'
            }),
        handler:
          (argv) => readPipe([
            Reader({
              type: 'random',
              width: argv.width
            })(argv.length)
          ],
          argv)
      })

      // kafka -> stdout
      .command({
        command: 'kafka <topic>',
        desc: 'Read from Kafka topic to stdout',
        builder:
          (yargs) => yargs
            .positional('topic', {
              describe: 'Kafka topic'
            })
            .option('broker', {
              alias: 'b',
              describe: 'Broker host(s) hostname:port',
              type: 'string',
              default: process.env.KPIPE_BROKERS
            })
            .option('groupid', {
              alias: 'g',
              describe: 'Consumer group id',
              type: 'string',
              default: 'kpipe-' + require('uid-safe').sync(6)
            })
            .option('partition', {
              alias: 'p',
              describe: 'Consume from partition',
              group: 'Offset options:',
              type: 'number'
            })
            .option('offset', {
              alias: 'o',
              describe: 'Start consuming at offset',
              group: 'Offset options:'
              // implies: ['partition']
            })
            .option('end', {
              alias: 'e',
              describe: 'Stop consuming at offset (inclusive)',
              group: 'Offset options:',
              type: 'numeric'
              // implies: ['offset']
            })
            .option('count', {
              alias: 'n',
              describe: 'Retrieve count number of events',
              group: 'Offset options:',
              type: 'numeric'
              // implies: ['offset']
            })
            .option('commit', {
              describe: 'Update consumer commit log',
              group: 'Offset options:',
              type: 'boolean'
            })
            .options('full', {
              describe: 'Return the full kafka message',
              type: 'boolean'
            })
            .conflicts('end', 'count')
            .coerce('offset', (arg) => {
              if (typeof arg === 'string' && ['stored', 'latest', 'earliest', 'beginning', 'end'].includes(arg)) {
                return arg
              }
              if (typeof arg === 'undefined') {
                return arg
              }
              const narg = parseInt(arg, 10)
              if (!isNaN(narg)) {
                return narg
              }
              throw new Error('offset must be numeric or one of [stored, latest, earliest, beginning, end]')
            }),
        handler:
          (argv) => {
            const position = {
              partition: argv.partition,
              offset: argv.offset,
              count: argv.end ? argv.end - argv.offset : argv.count
              // end: argv.end
            }
            return readPipe([
              Reader({
                type: 'kafka',
                groupid: argv.groupid,
                brokers: argv.broker,
                commit: argv.commit,
                debug: argv.debug,
                fullMessage: argv.full
              })(argv.topic, position),
              Transform.Lineate()
            ],
            argv)
          }
      })
}
