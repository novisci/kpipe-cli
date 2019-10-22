/***
 *
 */
const { PassThrough } = require('stream')
const { PartSequence, Transform } = require('kpipe-streams')
const { PipelinePromise } = require('kpipe-sequence')
const { Reader, Writer } = require('kpipe-core')

function exportPipe (topic, position, stream, argv = {}) {
  return require('../cli-cmd')(
    PipelinePromise(
      Reader({
        type: 'kafka',
        brokers: process.env.DPIPE_BROKERS
      })(topic, position),
      // require('../transform/progress')(),
      // require('../transform/lineate')(),
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

function streamParts (options) {
  if (typeof options.baseName !== 'string') throw Error('options.baseName is required')
  if (typeof options.limit !== 'number') throw Error('options.limit is required')
  if (typeof options.backend !== 'object') throw Error('options.backend is required')

  const baseName = options.baseName
  const limit = options.limit
  const backend = options.backend

  return PartSequence({
    baseName,
    bound: (state, chunk) => {
      if (!state.count) {
        state.count = 0
      }
      state.count++
      if (state.count > limit && chunk[0] !== state.lastSubject) {
        state.count = 0
        state.lastSubject = null
        return true
      }
      state.lastSubject = chunk[0]
      return false
    },
    backendOpts: backend
  })
}

module.exports = {
  command: 'export s3 <region> <bucket> <path> [options...] <topic>',
  desc: 'Export topic to stream',

  builder:
    (yargs) => yargs
      // .demandCommand()
      .positional('region', {
        describe: 'S3 region'
      })
      .positional('bucket', {
        describe: 'S3 bucket'
      })
      .positional('path', {
        describe: 'S3 object path'
      })
      .option('gzip', {
        type: 'boolean',
        describe: 'Compress using gzip'
      })
      .option('snappy', {
        type: 'boolean',
        describe: 'Compress using snappy'
      })
      .option('broker', {
        alias: 'b',
        describe: 'Broker host(s) hostname:port',
        type: 'string',
        default: process.env.DPIPE_BROKERS
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
      })
      .option('end', {
        alias: 'e',
        describe: 'Stop consuming at offset (inclusive)',
        group: 'Offset options:',
        type: 'numeric'
      })
      .option('count', {
        alias: 'n',
        describe: 'Retrieve count number of events',
        group: 'Offset options:',
        type: 'numeric'
      })
      .positional('topic', {
        describe: 'Kafka topic'
      }),

  handler:
      (argv) => {
        const position = {
          partition: argv.partition,
          offset: argv.offset,
          count: argv.end ? argv.end - argv.offset : argv.count
          // end: argv.end
        }
        return exportPipe(
          argv.topic,
          position,
          [
            compressStream(argv),
            Writer({
              type: 's3',
              region: argv.region,
              bucket: argv.bucket
            })(argv.path)
          ],
          argv
        )
      }
}
