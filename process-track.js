const memBaseline = process.memoryUsage().heapUsed + process.memoryUsage().external

const ProcessTracker = function (options) {
  if (!(this instanceof ProcessTracker)) {
    return new ProcessTracker(options)
  }
  options = options || {}

  const interval = options.interval || 10

  this._startTime = Date.now()
  this._min = this._max = memBaseline
  this._intvl = setInterval(() => this.collect(), interval)
  this._trace = typeof options.trace === 'undefined' ? false : options.trace

  if (this._trace) {
    let traceFile = 'trace.csv'
    if (typeof this._trace === 'string') {
      traceFile = this._trace
    }
    this._stream = require('fs').createWriteStream(traceFile)
  }

  process.on('SIGINT', (sig) => {
    process.stderr.write('\n')
    this.stop()
    require('wtfnode').dump()
    process.exit(sig)
  })
}

ProcessTracker.prototype.minimax = function (m) {
  this._min = Math.min(this._min, m)
  this._max = Math.max(this._max, m)
  return this
}

ProcessTracker.prototype.getMem = function () {
  const mem = process.memoryUsage()
  return mem.heapUsed + mem.external
}

ProcessTracker.prototype.collect = function () {
  this.minimax(this.getMem())
  if (this._trace) {
    this._stream.write(`${(Date.now() - this._startTime) * 0.001},${this.getMem() * 0.000001}\n`)
  }
  return this
}

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

ProcessTracker.prototype.stop = function () {
  clearInterval(this._intvl)

  if (this._trace) {
    this._stream.destroy()
  }

  const elapsed = Date.now() - this._startTime
  const peakMem = this._max - this._min

  const result = {
    elapsed,
    peakMem,
    human: {
      elapsed: humanTime(elapsed),
      peakMem: humanMemory(peakMem)
    }
  }

  console.info(' Elasped: ', result.human.elapsed)
  console.info('Peak mem: ', result.human.peakMem)

  return result
}

module.exports = ProcessTracker
