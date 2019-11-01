/***
 * Create a global logger which can be redirected or switched off
 */
module.exports = function ({ quiet, verbose, debug } = {}) {
  const isInfo = (true && !quiet)
  const isDebug = (debug && !quiet)

  console.info = console.error
  console.debug = console.error

  if (!isInfo) {
    console.info = () => {}
  }

  if (!isDebug) {
    console.debug = () => {}
  }
}
