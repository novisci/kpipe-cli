/***
 * Wrapper around cli commands which handles common setup/teardown
 */
const producer = require('kpipe-core').KafkaProducer
module.exports = function (promise, argv = {}) {
  let tracker
  if (argv.trace) {
    tracker = require('./process-track')({ trace: argv.trace })
  }
  const startStats = producer.stats()
  return promise
    .then(() => {
      if (tracker) {
        tracker.stop()
      }
      console.info('Producer:')
      console.info(producer.deltaStats(startStats))
    })
}
