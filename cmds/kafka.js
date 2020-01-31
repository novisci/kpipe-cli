const { KafkaProducer, KafkaAdmin } = require('kpipe-core')
const { PromiseChain } = require('kpipe-sequence')

/**
 * Render a numeric array as compact ranges
 * eg. [0,1,2,3,5,6,7,8,10,13,15,16,17] => '0-3,5-8,10,13,15-17'
 */
function renderSeq (seq) {
  if (seq.length <= 0) return ''
  if (seq.length === 1) return `${seq[0]}`

  let s = `${seq[0]}`
  let n = seq[0]
  seq.sort((a, b) => a - b)
  let l = seq.reduce((a, c) => {
    if (c - a > 1) {
      if (a - n > 1) {
        s += `-${a}`
      }
      s += `,${c}`
      n = c
    }
    return c
  }, n)
  if (l - n > 1) {
    s += `-${l}`
  } else {
    s += `,${l}`
  }
  return s
}

/**
 * Show topic leaders and the portitions they lead
 */
// function summarizePartitions (ps) {
//   const s = {}
//   ps.map((p) => {
//     if (!s[p.leader]) s[p.leader] = []
//     s[p.leader].push(p.id)
//   })
//   let str = ''
//   Object.entries(s).map((e) => {
//     str += `${e[0]}/${renderSeq(e[1])} `
//   })
//   return str
// }

async function summarizePartitions (client, topic, parts, fn, options) {
  fn(`  ${topic}:`)
  return PromiseChain({}, ...parts.map((p) => async () => new Promise((resolve) => {
    client.queryWatermarkOffsets(topic, p.id, 1000, (err, off) => {
      if (err) {
        fn(`    ${p.id} [ERROR] (${p.leader}) ${err.message}`)
      } else {
        if (options.human) {
          fn(`    ${p.id} [${off.lowOffset.toLocaleString()} - ${off.highOffset.toLocaleString()}] (${p.leader})`)
        } else {
          fn(`    ${p.id} [${off.lowOffset} - ${off.highOffset}] (${p.leader})`)
        }
      }
      resolve()
    })
  })))
}

async function summarizeTopic (client, topic, parts, fn, options) {
  fn(`  ${topic}:`)
  const numParts = parts.length
  let numMsgs = 0
  return PromiseChain({}, ...parts.map((p) => async () => new Promise((resolve) => {
    client.queryWatermarkOffsets(topic, p.id, 1000, (err, off) => {
      if (err) {
        fn(`    ${p.id} [ERROR] (${p.leader}) ${err.message}`)
      } else {
        numMsgs += off.highOffset - off.lowOffset
      }
      resolve()
    })
  })))
    .then(() => {
      if (options.human) {
        fn(`    ${numParts} [${numMsgs.toLocaleString()}]`)
      } else {
        fn(`    ${numParts} [${numMsgs}]`)
      }
    })
}

async function summarizeTopics (client, topics, fn, options) {
  return PromiseChain({}, ...topics.map((t) => async () => {
    // if (t.name === '__consumer_offsets') {
    //   return Promise.resolve()
    // }
    if (options.all) {
      await summarizePartitions(client, t.name, t.partitions, fn, options)
    } else {
      await summarizeTopic(client, t.name, t.partitions, fn, options)
    }
  }))
}

// function showOffsets (client, topic) {
//   const off = client.queryWatermnarkOffsets(topic, 0)
// }

/**
 * Describe broker metadata using the supplied output function
 */
async function renderMetadata (client, meta, fn, options) {
  options = options || {}

  fn(`orig_broker_id: ${meta.orig_broker_id}`)
  fn(`orig_broker_name: ${meta.orig_broker_name}`)
  fn('topics:')
  // meta.topics.map((t) => {
  //   fn(`  ${t.name}:`)
  //   summarizePartitions(client, t.name, t.partitions, fn)
  // })
  await summarizeTopics(client, meta.topics, fn, options)
  fn('brokers:')
  meta.brokers.map((b) => fn(`  ${b.id}: ${b.host}:${b.port}`))
}

module.exports = {
  command: 'kafka <cmd> [options...]',
  desc: 'Kafka cluster administration',

  builder:
    (yargs) => yargs
      .option('broker', {
        alias: 'b',
        describe: 'Broker host(s) hostname:port',
        type: 'string',
        default: process.env.KPIPE_BROKERS
      })
      .demandCommand()

      // Broker/topic info
      .command({
        command: 'ls',
        desc: 'Show topic and broker information',
        builder:
          (yargs) => yargs
            .option('all', {
              alias: 'a',
              type: 'boolean',
              describe: 'Show all information for topics'
            })
            .option('human', {
              alias: 'h',
              type: 'boolean',
              describe: 'Display human readable counts',
              default: false
            }),
        handler:
          (argv) => {
            KafkaProducer.connect({
              brokers: argv.broker
            })
              .then((producer) => {
                return renderMetadata(producer, KafkaProducer.metadata(), console.log, {
                  all: argv.all,
                  human: argv.human
                })
                  .catch(console.error)
                  .then(() => KafkaProducer.disconnect())
              })
              .catch(console.error)
          }
      })

      // Create topic
      .command({
        command: 'create <topic>',
        desc: 'Create new topic',
        builder:
          (yargs) => yargs
            .positional('topic', {
              type: 'string',
              describe: 'Topic name',
              demand: true
            })
            .option('partitions', {
              alias: 'p',
              describe: 'Number of partitions',
              type: 'numeric'
            })
            .option('replicas', {
              alias: 'r',
              describe: 'Number of replicas',
              type: 'numeric'
            })
            .option('compact', {
              describe: 'Create a compacted topic',
              type: 'boolean'
            }),
        handler:
          (argv) => {
            const admin = KafkaAdmin({
              brokers: argv.broker
            })
            const options = {}
            if (argv.compact) {
              options['cleanup.policy'] = 'compact'
            }
            admin.createTopic(argv.topic, argv.partitions, argv.repliacs, options)
              .then(() => console.info(`Created topic ${argv.topic}`))
              .catch(console.error)
              .then(() => admin.disconnect())
          }
      })

      // Delete topic
      .command({
        command: 'delete <topic>',
        desc: 'Delete a topic',
        builder:
          (yargs) => yargs
            .positional('topic', {
              type: 'string',
              describe: 'Topic name',
              demand: true
            }),
        handler:
          (argv) => {
            // if (argv.topic === '__consumer_offsets') {
            //   console.error(`Don't delete ${argv.topic}`)
            //   return
            // }
            const admin = KafkaAdmin({
              brokers: argv.broker
            })
            admin.deleteTopic(argv.topic)
              .then(() => console.info(`Deleted topic ${argv.topic}`))
              .catch(console.error)
              .then(() => admin.disconnect())
          }
      })

      // Partition topic
      .command({
        command: 'partition <topic> <total>',
        desc: 'Create topic partitions',
        builder:
          (yargs) => yargs
            .positional('topic', {
              type: 'string',
              describe: 'Topic name',
              demand: true
            })
            .positional('total', {
              type: 'number',
              describe: 'Total number of partitions',
              demand: true
            }),
        handler:
            (argv) => {
              // if (argv.topic === '__consumer_offsets') {
              //   console.error(`Don't partiton ${argv.topic}`)
              //   return
              // }
              const admin = KafkaAdmin({
                brokers: argv.broker
              })
              admin.createPartitions(argv.topic, argv.total)
                .then(() => console.info(`Created ${argv.total} partitions on ${argv.topic}`))
                .catch(console.error)
                .then(() => admin.disconnect())
            }
      })
}
