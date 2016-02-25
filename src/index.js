"use strict";

const bytespace = require('bytespace')
    , bytewise = require('bytewise-core')
    , bytewiseHex = require('bytewise/encoding/hex')
    , through2 = require('through2')
    , t = require('mini-type-assert')
    , Emitter = require('events').EventEmitter
    , timestamp = require('monotonic-timestamp')
    , kindOf = require('kindof')
    , eos = require('end-of-stream')
    , pump = require('pump')
    , fastFuture = require('fast-future')
    , NotFoundError = require('level-errors').NotFoundError
    , probe = require('level-probe')
    , duplexify = require('duplexify')
    , writer = require('flush-write-stream')
    , autobind = require('autobind-decorator')
    , skipStream = require('./skip-stream')
    , batchStream = require('./batch-stream')
    , liveStream = require('./live-stream')
    , iteratorStream = require('level-iterator-stream')
    , noop = function() {}

const LO = bytewise.bound.lower()
    , HI = bytewise.bound.upper()

const SEP = '.'
    , SYNC_SIGNAL = 'SYNC'

module.exports = function factory(id, db, opts) {
  return new SpaceShuttle(id, db, opts)
}

class SpaceShuttle extends Emitter {
  constructor(id, db, opts = {}) {
    super();

    if (typeof id !== 'string' || !id) {
      throw new Error('A source ID is required')
    }

    // For test purposes (because proxyquireify broke browser tests)
    this.timestamp = opts.timestamp || timestamp

    this.id = id
    this.setMaxListeners(Number.MAX_VALUE)
    this.future = fastFuture()

    // Create root namespace
    this.db = bytespace(db, opts.ns || 'space-shuttle', {
      keyEncoding: bytewiseHex,
      valueEncoding: 'json',
      hexNamespace: true
    })

    // For traversal. Ordered by path, newest first, source:
    //  props<[path, negative_ts, source, erased], prop_value>
    this.props = this.db.sublevel('props')

    // For replication. Ordered by source, oldest first, path:
    //  inverse<[source, ts, path, erased], prop_value>
    this.inverse = this.db.sublevel('inverse')

    // For faster replication and ignoring old updates:
    //  clock<source, latest_ts>
    this.clock = this.db.sublevel('clock')

    // In-memory equivalent of this.clock
    this.memoryClock = Object.create(null)

    // Install a hook to insert and clean up inverse props
    this.props.pre((op, add) => {
      const { type, key: [ path, neg, source, erased ], value } = op
      const inverseKey = [ source, -neg, path, erased ]

      if (type === 'del') {
        add({ prefix: this.inverse, type: 'del', key: inverseKey })
      } else {
        add({ prefix: this.inverse, type: 'put', key: inverseKey, value })
      }
    })

    this.queue = []
    this.db.on('close', this.close)

    if (this.db.isOpen()) this.open()
    else this.db.once('open', this.open)
  }

  @autobind open() {
    // Read clock into memory
    const s = liveStream(this.clock).on('data', ({ key: source, value: ts }) => {
      this.memoryClock[source] = ts
    }).once('sync', () => {
      // Scuttlebutt streams and batches are deferred until now
      this.ready = true
      this.emit('ready')
    }).once('error', this.emit.bind(this, 'error'))

    this.once('close', s.end.bind(s))
  }

  @autobind close() {
    if (this.closed) return
    if (this.queue.length) return this.once('drain', this.close)

    this.closed = true
    this.emit('close')
  }

  // Read history since clock<source, ts>
  historyStream(opts) {
    const { tail = false, compat = true } = opts || {}
    const clock = opts && opts.clock ? { ...opts.clock } : {} // Clone, b/c we mutate

    const output = through2.obj(function({ key, value }, _, next) {
      const [ source, ts, path, erased ] = key

      if (compat) { // Convert to dc's scuttlebutt format
        const trx = [ join(path), erased ? undefined : value ]
        next(null, [ trx, ts, source ])
      } else {
        next(null, { source, ts, path, erased, value })
      }
    })

    // Stream sorted by timestamps, then sources
    const skipper = skipStream(this.inverse, this.clock, { clock })

    eos(skipper, (err) => {
      if (err) return output.destroy(err)

      if (!tail) {
        output.emit('sync')
        return this.future(output.end.bind(output))
      }

      // Skip updates if remote already has them
      const filter = through2.obj(function(kv, _, next) {
        const [ source, ts ] = kv.key
        if (clock[source] && clock[source] >= ts) return next()
        else clock[source] = ts
        next(null, kv)
      })

      pump( liveStream(this.inverse, { old: false, tail: true })
          , filter
          , output )

      output.emit('sync')
    })

    return skipper.pipe(output, { end: false })
  }

  defer(onOpen, onClose) {
    const start = () => {
      this.removeListener('close', cancel)
      this.future(onOpen)
    }

    const cancel = () => {
      this.removeListener('ready', start)
      this.future(onClose)
    }

    this.once('ready', start)
    this.once('close', cancel)
  }

  // Duplex replication stream
  replicate(opts = {}, _d) {
    const d = _d || duplexify.obj(null, null, { allowHalfOpen: true, destroy: false })
    d.name = opts.name

    if (!this.ready) {
      this.defer(this.replicate.bind(this, opts, d), d.destroy.bind(d))
      return d
    }

    const writable = opts.writable !== false // default to true
        , readable = opts.readable !== false
        , tail = opts.tail !== false

    let syncRecv = !writable
      , syncSent = !readable

    const sync = () => {
      d.emit('sync')
      if (!tail) this.future(d.dispose)
    }

    const sendHistory = (clock, cb) => {
      const opts = { compat: true, tail, clock }

      d.setReadable(this.historyStream(opts).once('sync', () => {
        d.push(SYNC_SIGNAL)

        syncSent = true
        d.emit('syncSent')

        // When we have received remote's history
        if (syncRecv) sync()
        if (cb) cb()
      }))
    }

    const batcher = batchStream(this)
    const incoming = through2.obj((data, _, next) => {
      if (Array.isArray(data)) { // It's an update
        if (writable && validate(data)) {
          // TODO: write test to simulate late batch, early stream end
          const patch = this.convertScuttlebuttUpdate(data)
          return next(null, patch)
        }
      } else if ('object' === typeof data && data) { // It's a digest
        if (syncSent) return next()
        else if (validate.digest(data)) return sendHistory(data.clock, next)
        else return next(new Error('Invalid digest'))
      } else if (data === SYNC_SIGNAL) {
        syncRecv = true
        d.emit('syncReceived')
        if (syncSent) sync()
      }

      next()
    }, function flush(cb){
      eos(batcher, cb)
      process.nextTick(batcher.end.bind(batcher))
    })

    incoming.pipe(batcher)
    d.setWritable(incoming)

    const destroy = d.destroy
    d.dispose = d.destroy = (err) => {
      this.removeListener('close', d.dispose)
      d.removeListener('finish', d.dispose)

      if (d.disposed) return
      d.disposed = true

      if (d._readable) d._readable.end()
      if (err) destroy.call(d, err)
    }

    d.on('finish', d.dispose)
    this.on('close', d.dispose)

    // Send my current clock so the other side knows what to send.
    // Clone the clock, because scuttlebutt mutates this object.
    const digest = { id: this.id, clock: { ...this.memoryClock } }
    if (opts.meta) digest.meta = opts.meta

    if (readable) {
      d.push(digest)

      // What's this?
      if (!writable && !opts.clock) sendHistory()
    } else if (opts.sendClock) {
      d.push(digest)
    }

    return d
  }

  // Temporary compatibility with dc's scuttlebutt/model
  convertScuttlebuttUpdate(update) {
    const [ trx, ts, source ] = update
    const [ path, value ] = trx
    return { source, ts, path, value }
  }

  sublevel(prefix) {
    // TODO
  }

  put(path, value, options, cb) {
    this.batch([{ path, value }], options, cb)
  }

  batch(patches, options, cb) {
    if (typeof options === 'function') cb = options, options = {}

    this.queue.push([ patches, cb, options ])

    if (!this.writing) {
      this.writing = true
      this._nextBatch()
    }
  }

  // this is overengineered, but keeping it for now b/c tests rely on it
  @autobind _nextBatch() {
    if (!this.ready) {
      return this.defer(this._nextBatch, () => {
        this.emit('error', new Error('premature close, pending batch'))
      })
    }

    const batch = []
        , props = {}
        , newClock = {}
        , callbacks = []

    for(let i=0, l=this.queue.length; i<l; i++) {
      const [ patches, cb, options ] = this.queue[i]
      if (cb) callbacks.push(cb)
      this._writePatches(batch, props, newClock, patches, options)
    }

    this.queue = []

    const next = (err) => {
      if (callbacks.length) callbacks.forEach(f => f(err))
      else if (err) this.emit('error', err)

      if (!this.queue.length) {
        this.writing = false
        if (batch.length > 0) this.emit('drain')
      } else {
        this.future(this._nextBatch)
      }
    }

    if (batch.length === 0) return this.future(next)

    // Update clocks to latest in batch. Add to beginning, so
    // that this.memoryClock is updated (with post hook)
    // before any open historyStreams receive the updates
    const prepend = Object.keys(newClock).map(source => {
      return { prefix: this.clock, key: source, value: newClock[source] }
    })

    this.db.batch(prepend.concat(batch), next)
  }

  erase(path, cb) {
    pump( this.readStream({ path, values: false })
        , through2.obj(function(key, _, next){
            return next(null, { path: key[0], erased: true })
          })
        , batchStream(this)
        , cb)
  }

  get(path, opts, cb) {
    if (typeof opts === 'function') cb = opts, opts = {}
    path = explode(path)

    // Ignore keys and values options
    const { keys, values, erased, ...range } = opts

    // TODO: path.concat(HI)?
    range.gt = [path, LO]
    range.lt = [path, HI]

    probe(this.props, range, function(err, kv){
      if (err && err.notFound) return cb(notFound(path))
      else if (err) return cb(err)

      if (kv.key[3] && !erased) cb(notFound(path))
      else cb(null, kv.value)
    })
  }

  iterator(opts = {}) {
    const { keys = true, values = true, paths
          , path, old, erased, ...range } = opts

    if (path) {
      range.gt = [explode(path)]
      range.lt = [range.gt[0].concat(HI)]
    }

    range.keys = true // Always read keys
    range.values = values

    let prev = []
      , props = this.props
      , iter = props.iterator(range)

    return {
      next: function(cb) {
        const handle = function(err, key, value) {
          if (err || key === undefined) return cb(err)

          if (!old) { // Skip same (older) paths
            if (pathEquals(key[0], prev)) {
              iter.end(noop)

              // Skip ahead to next path
              // TODO: this should be done natively with iterator#seek.
              range.gt = [prev, HI]
              return (iter = props.iterator(range)).next(handle)
            }

            prev = key[0]
          }

          if (key[3] && !erased) return iter.next(handle)

          cb(null, paths ? key[0] : keys ? key : null, values ? value : null)
        }

        iter.next(handle)
      },

      end: function(cb) {
        iter.end(cb)
      }
    }
  }

  readStream(opts = {}) {
    const { keys = true, values = true } = opts
    const decoder = keys && values ? void 0 : keys ? keyOnly : valueOnly
    return iteratorStream(this.iterator(opts), { decoder, ...opts })
  }

  // TODO: support patch.type = del for level* compatibility
  _writePatches(batch, props, newClock, patches, options = {}) {
       // For test purposes: if false, old updates are not ignored
    const filter = options.filter !== false
        , defSource = options.source || this.id

    patchLoop: for(let i=0, l=patches.length; i<l; i++) {
      const patch = patches[i]
        , { source = defSource, ts = this.timestamp(), value } = patch
          , path = explode(patch.path == null ? patch.key : patch.path)
          , erased = value == null || patch.erased === true

      // Validate
      if (!Array.isArray(path) || path.length === 0) {
        // Emit a normalized patch
        this.emit('invalid', { path, ts, source, value, erased })
        continue
      }

      for(let i=0, l=path.length; i<l; i++) {
        const t = typeof path[i]

        if (path[i] === '__proto__' || (t !== 'number' && t !== 'string')) {
          this.emit('invalid', { path, ts, source, value, erased })
          continue patchLoop
        }
      }

      if (typeof ts !== 'number' || typeof source !== 'string') {
        this.emit('invalid', { path, ts, source, value, erased })
        continue
      }

      if (filter) {
        const id = path.join(SEP)

        // Ignore old and out of order updates
        if (props[id] > ts || this.memoryClock[source] >= ts
              || newClock[source] > ts) {
          this.emit('old', { path, ts, source, value, erased })
          continue
        }

        // this.memoryClock is updated by post hook
        props[id] = ts
      }

      newClock[source] = ts

      batch.push( { prefix: this.props, key: [ path, -ts, source, erased ]
                  , value: erased ? '' : value } )
    }
  }

  writeStream() {
    // TODO: batchStream(this)
  }

  // TODO: if ts of ancestor (latest ts in branch) is > ours, ignore child?
  tree(path, done) {
    if (typeof path === 'function') done = path, path = []

    const prefixLength = explode(path).length
    const root = {}

    pump(
      this.readStream({ path }),
      writer.obj(function({ key, value }, _, next){
        let path = key[0].slice(prefixLength), node = root

        if (!path.length) path.push('_')

        for(let i=0, l=path.length; i<l; i++) {
          const seg = path[i], type = kindOf(node[seg])

          if (i === path.length - 1) node[seg] = value
          else if (type === 'undefined') node = node[seg] = {}
          else if (type !== 'object') node = node[seg] = { _: node[seg] }
          else node = node[seg]
        }

        next()
      }),
      function(err) {
        done(err, root)
      }
    )
  }
}

// Aliases
SpaceShuttle.prototype.createReadStream = SpaceShuttle.prototype.readStream
SpaceShuttle.prototype.createWriteStream = SpaceShuttle.prototype.writeStream
SpaceShuttle.prototype.createStream = SpaceShuttle.prototype.replicate

SpaceShuttle.pathEquals = pathEquals

function explode(path) {
  if (typeof path === 'number') return [path]
  else if (typeof path === 'string') return path.split(SEP)
  else return path
}

function join(path) {
  if (typeof path === 'number') return ''+path
  else if (typeof path === 'string') return path
  else return path.join(SEP)
}

function pathEquals(a, b) {
  let l = a.length
  if (l !== b.length) return false
  for(; l--;) if (a[l] !== b[l]) return false
  return true
}

function notFound(k) {
  return new NotFoundError('Key not found in database [' + join(k) + ']')
}

function keyOnly(key) { return key }
function valueOnly(_, value) { return value }

// Taken from dc's scuttlebutt
function validate (data) {
  if (!(Array.isArray(data)
    && 'string' === typeof data[2]
    && '__proto__'     !== data[2] // this would break stuff
    && 'number' === typeof data[1]
  )) return false

  return true
}

validate.digest = function(digest) {
  return digest && typeof digest.clock === 'object' ? true : false
}
