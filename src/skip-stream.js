const iterator = require('level-iterator')
    , bytewise = require('bytewise-core')
    , sorted = require('sorted')
    , Readable = require('readable-stream').Readable
    , noop = function(){}

const LO = bytewise.bound.lower()
    , HI = bytewise.bound.upper()

// A stream sorted by timestamps, then sources.
//
// Does a heap-sorted skip scan on
//   db<[source, ts, path, erased], prop_value>
//
// TODO: external merge sort
function skipStream(db, clockDb, opts) {
  const { clock = {}, highWaterMark = 16 } = opts

  const distinct = []
      , heap = sorted([], cmp)
      , stream = PullReadable.obj({ highWaterMark }, pull)

  // Could collect distinct sources on `db` using
  // `level-probe`, but this is faster.
  clockDb.keyStream()
    .on('data', function(key) { distinct.push(key) })
    .on('end', iterate)
    .on('error', stream.destroy.bind(stream))

  return stream

  // On each leading edge (source), iterate predicate edge range (ts)
  function iterate() {
    if (!distinct.length) return stream.push(null)

    distinct.forEach(function(source){
      const ts = clock[source] || LO
      next(db.iterator({ gt: [source, ts, HI], lt: [source, HI] }))
    })
  }

  function next(iter) {
    iter.next(function(err, key, value){
      if (err) return stream.destroy(err)

      if (key === undefined) {
        if (--distinct.length === 0) { // Flush and end
          heap.forEach(entry => stream.push(entry[1]))
          stream.push(null)
        } else if (heap.length) {
          stream.pull()
        }

        return iter.end(noop)
      }

      try {
        // Sorting on key[1], which is "ts", the predicate edge
        heap.push([key[1], { key, value }, iter ])
      } catch(err) {
        return stream.destroy(err)
      }

      // Write when we have an element from each iterator
      if (heap.length === distinct.length) stream.pull()
    })
  }

  function pull() {
    // Take the first (smallest) element from the heap, push kv, then refill
    const [ _, kv, iter ] = heap.shift()
    next(iter)
    return kv
  }
}

module.exports = skipStream

function cmp ([a], [b]) {
  if (a === b) return 0
  else if (a > b) return 1
  else if (a < b) return -1
  else throw new RangeError('Unstable comparison: ' + a + ' cmp ' + b)
}

class PullReadable extends Readable {
  static obj(opts, pull) {
    if (typeof opts === 'function') pull = opts, opts = {}
    opts = { highWaterMark: 16, ...opts, objectMode: true }
    return new PullReadable(opts, pull)
  }

  constructor(opts, pull) {
    if (typeof opts === 'function') pull = opts, opts = {}
    super(opts)

    this.destroyed = false

    this._reading = false
    this._pendingPull = false
    this._pull = pull
  }

  // Consumer must call pull()
  pull() {
    if (this.destroyed) return

    if (this._reading) {
      this._pendingPull = false

      const chunk = this._pull()

      if (chunk === undefined) return
      else if (chunk === null) this.push(chunk)
      else if (!this.push(chunk)) this._reading = false
    } else {
      this._pendingPull = true
    }
  }

  _read() {
    if (this._reading || this.destroyed) return
    this._reading = true
    if (this._pendingPull) this.pull()
  }

  destroy(err) {
    if (this.destroyed) return
    this.destroyed = true

    process.nextTick(() => {
      if (err) this.emit('error', err)
      this.emit('close')
    })
  }
}
