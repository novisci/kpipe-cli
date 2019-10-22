/***
 * Create a global logger which can be redirected or switched off
 */
module.exports = function ({ quiet, verbose, debug } = {}) {
  const isInfo = (verbose && !quiet)
  const isDebug = (debug && !quiet)

  if (!isInfo) {
    console.info = () => {}
  }

  if (!isDebug) {
    console.debug = () => {}
  }
}
