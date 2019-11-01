function humanCount (count, div, units) {
  let c = count
  let i = 0
  while (c >= div[i] && i < units.length - 1) {
    c /= div[i]
    i++
  }
  return c.toFixed(3).toLocaleString().padStart(8, ' ') + ' ' + units[i]
}

function humanTime (ms) {
  return humanCount(ms, [1000, 60, 60, 24], ['ms', 's', 'm', 'h', 'd'])
}

function humanMemory (bytes) {
  return humanCount(bytes, [1024, 1024, 1024, 1024, 1024], ['b', 'Kb', 'Mb', 'Gb', 'Tb'])
}

module.exports = {
  humanCount,
  humanTime,
  humanMemory
}
