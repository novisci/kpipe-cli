/***
 *
 */
const { PassThrough } = require('stream')
const { PipelinePromise } = require('kpipe-sequence')
const { Transform } = require('kpipe-streams')
const { Reader, Writer } = require('kpipe-core')

function writePipe (stream, argv = {}) {
  return require('../cli-cmd')(
    PipelinePromise(
      Reader({ type: 'stdio' })(),
      // Transform.Delineate(),
      // require('../../../src/transform/progress')(),
      ...stream
    )
      .catch((err) => console.error(err)),
    argv
  )
}

function compressStream (argv) {
  if (argv.gzip) {
    return Transform.Gzip()
  } else if (argv.snappy) {
    return Transform.SnappyCompress()
  } else {
    return new PassThrough()
  }
}

module.exports = {
  command: 'write <sink> [options...]',
  desc: 'Write to stream from stdin',

  builder:
    (yargs) => yargs
      .demandCommand()
      .option('gzip', {
        alias: 'z',
        type: 'boolean',
        describe: 'Compress using gzip'
      })
      .option('snappy', {
        type: 'boolean',
        describe: 'Compress using snappy'
      })

      // stdin -> stdout
      .command({
        command: 'stdio',
        desc: 'Write to file system from stdin',
        builder:
          (yargs) => yargs,
        handler:
          (argv) => writePipe([
            // Transform.Lineate(),
            compressStream(argv),
            Writer({ type: 'stdio' })()
          ],
          argv)
      })

      // stdin -> fs
      .command({
        command: 'fs <filename>',
        desc: 'Write to file system from stdin',
        builder:
          (yargs) => yargs
            .positional('filename', {
              describe: 'Path to file',
              type: 'string'
            }),
        handler:
          (argv) => writePipe([
            // Transform.Lineate(),
            compressStream(argv),
            Writer({ type: 'fs' })(argv.filename)
          ],
          argv)
      })

      // stdin -> s3
      .command({
        command: 's3 <region> <bucket> <path>',
        desc: 'Write to S3 from stdin',
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
          (argv) => writePipe([
            // Transform.Lineate(),
            compressStream(argv),
            Writer({
              type: 's3',
              region: argv.region,
              bucket: argv.bucket
            })(argv.path)
          ],
          argv)
      })

      // stdin -> kafka
      .command({
        command: 'kafka [options...] <topic>',
        desc: 'Write to Kafka topic from stdin',
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
            }),
        handler:
          (argv) => {
            const position = {
              partition: argv.partition,
              offset: argv.offset,
              count: argv.end ? argv.end - argv.offset : argv.count
              // end: argv.end
            }

            return writePipe([
              Transform.Delineate(),
              Writer({
                type: 'kafka',
                brokers: argv.broker,
                objectMode: true
              })(argv.topic, position)
            ],
            argv)
          }
      })
}
