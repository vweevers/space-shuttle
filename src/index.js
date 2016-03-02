"use strict";

const bytespace = require('bytespace')
    , bytewise = require('bytewise-core')
    , bytewiseHex = require('./hex')
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
    , merge2 = require('merge2')
    , noop = function() {}

const LO = bytewise.bound.lower()
    , HI = bytewise.bound.upper()

const SEP = '.'
    , SYNC_SIGNAL = 'SYNC'
    , READONLY_SIGNAL = 'READONLY'

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
    this.unhookPre = this.props.pre((op, add) => {
      // Destructuring is nice but babel needlessly adds iterator support
      const path   = op.key[0], neg    = op.key[1]
          , source = op.key[2], erased = op.key[3]

      const inverseKey = [ source, -neg, path, erased ]

      if (op.type === 'del') {
        add({ prefix: this.inverse, type: 'del', key: inverseKey })
      } else {
        add({ prefix: this.inverse, type: 'put', key: inverseKey, value: op.value })
      }
    })

    this.db.on('close', this.close)
    if (this.db.isOpen()) this.open()
    else this.db.once('open', this.open)
  }

  @autobind open() {
    // Read clock into memory
    const update = (op) => {
      if (op.type !== 'del') {
        this.memoryClock[op.key] = op.value // clock<source, ts>
      }
    }

    this.clock.createReadStream()
      .on('data', update)
      .on('error', this.errback)
      .on('end', () => {
        const unhook = this.clock.post(update)
        this.once('close', unhook)

        // Scuttlebutt streams and batches are deferred until now
        this.ready = true
        this.emit('ready')
      })
  }

  @autobind close() {
    if (!this.closed) {
      this.unhookPre()
      this.closed = true
      this.emit('close')
    }
  }

  // Read history since clock<source, ts>
  // TODO: test that s.end() removes the hook
  historyStream(opts) {
    const { tail = false, compat = true } = opts || {}
    const clock = opts && opts.clock ? { ...opts.clock } : {} // Clone, b/c we mutate

    const outer = through2.obj(function({ key, value }, _, next) {
      const source = key[0], ts = key[1], path = key[2], erased = key[3]

      if (compat) { // Convert to dc's scuttlebutt format
        const trx = [ join(path), erased ? undefined : value ]
        next(null, [ trx, ts, source ])
      } else {
        next(null, { source, ts, path, erased, value })
      }
    })

    // Stream sorted by timestamps, then sources
    const skipper = skipStream(this.inverse, this.clock, { clock })
    const streams = [skipper]

    eos(skipper, { readable: true, writable: false }, (err) => {
      if (err) outer.destroy(err)
      else outer.emit('sync')
    })

    if (tail) {
      const live = liveStream(this.inverse, { old: false, tail: true })

      // Skip updates if remote already has them
      streams.push(live.pipe(through2.obj(function filter(kv, _, next) {
        const source = kv.key[0], ts = kv.key[1]
        if (clock[source] && clock[source] >= ts) return next()
        else clock[source] = ts
        next(null, kv)
      })))
    }

    return merge2(...streams, { objectMode: true, end: false })
      .on('queueDrain', () => this.future(outer.end.bind(outer))) // Delay end
      .pipe(outer)
  }

  defer(onOpen, onClose) {
    const open = () => {
      if (onClose) this.removeListener('close', close)
      this.future(onOpen)
    }

    const close = () => {
      if (onOpen) this.removeListener('ready', open)
      this.future(onClose)
    }

    if (onOpen) this.once('ready', open)
    if (onClose) this.once('close', close)
  }

  // Duplex replication stream
  replicate(opts = {}, _d) {
    const d = _d || duplexify.obj(null, null, { allowHalfOpen: true, destroy: false })
    d.name = opts.name || ('replicate_' + this.id)

    if (!this.ready) {
      this.defer(this.replicate.bind(this, opts, d), () => { d.destroy() })
      return d
    }

    // Default to true
    const writable = opts.writable !== false
        , readable = opts.readable !== false
        , tail = opts.tail !== false

    let syncRecv = false
      , syncSent = false

    // We're readable and other side is writable
    const sendHistory = (clock, next) => {
      const opts = { compat: true, tail, clock }

      d.setReadable(this.historyStream(opts).once('sync', () => {
        sync('send')
        next()
      }))
    }

    // We're not readable or other side is not writable
    const skipHistory = (isVoid, cb) => {
      process.nextTick(() => {
        sync('send')

        // End if both sides are not writable
        if (isVoid || !tail) d.setReadable(null)
        else d.setReadable(through2.obj())

        cb && cb()
      })
    }

    // Handle signals and updates
    const batcher = batchStream(this, (data, next) => {
      if (Array.isArray(data)) { // It's an update
        if (!writable) next(new Error('Not writable'))
        else if (!validate(data)) next(new Error('Invalid update'))
        else {
          const patch = this.convertScuttlebuttUpdate(data)
          return this.filter(patch) ? next(null, patch) : next()
        }
      } else if ('object' === typeof data && data) { // It's a digest
        if (syncSent) next(new Error('Sync signal already sent'))
        else if (!readable) skipHistory(true, next)
        else if (validate.digest(data)) sendHistory(data.clock, next)
        else next(new Error('Invalid digest'))
      } else if (data === SYNC_SIGNAL) {
        if (syncRecv) next(new Error('Sync signal already received'))
        else sync('receive', next)
      } else if (data === READONLY_SIGNAL) {
        skipHistory(false, next)
      } else {
        next(new Error('Invalid data'))
      }
    })

    d.setWritable(batcher.on('commit', d.emit.bind(d, 'commit')))

    function sync(direction, cb) {
      if (direction === 'send') {
        d.push(SYNC_SIGNAL)
        syncSent = true
        d.emit('syncSent')
      } else {
        syncRecv = true
        d.emit('syncReceived')
      }

      if (syncSent && syncRecv) {
        batcher.commit(() => {
          // Fully consumed each other's history. Switch to tick batching
          batcher.setWindow(1)
          d.emit('sync')
        })
      }

      if (cb) cb()
    }

    const destroy = d.destroy
    const onclose = d.end.bind(d)

    // Make sure the readable side ends
    const dispose = d.destroy = (err) => {
      this.removeListener('close', onclose)
      d.removeListener('finish', dispose)

      if (d.disposed) return
      d.disposed = true

      if (d._readable) d._readable.end()
      if (err) destroy.call(d, err)
    }

    d.on('finish', dispose)
    this.on('close', onclose)

    if (writable) {
      // Send my current clock so the other side knows what to send.
      const digest = { id: this.id, clock: { ...this.memoryClock } }
      if (opts.meta) digest.meta = opts.meta
      d.push(digest)
    } else {
      d.push(READONLY_SIGNAL)
    }

    return d
  }

  filter(patch) {
    if (!patch.source) throw new Error('Missing source on patch')
    if (!patch.ts) throw new Error('Missing timestamp on patch')

    if (this.memoryClock[patch.source] >= patch.ts) {
      this.emit('old', patch)
      return false
    }

    return true
  }

  // Temporary compatibility with dc's scuttlebutt/model
  convertScuttlebuttUpdate(update) {
    const path = update[0][0]
        , value = update[0][1]
        , ts = update[1]
        , source = update[2]

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

    if (!this.ready) {
      return this.defer(() => { this.batch(patches, options, cb) })
    }

    const batch = [], newClock = {}
    this._writePatches(batch, newClock, patches, options)
    if (batch.length === 0) return cb && this.future(cb)

    // Update clocks to latest in batch. Add to beginning, so
    // that this.memoryClock is updated (with post hook)
    // before any open historyStreams receive the updates
    const prepend = Object.keys(newClock).map(source => {
      return { prefix: this.clock, key: source, value: newClock[source] }
    })

    this.db.batch(prepend.concat(batch), cb || this.errback)
  }

  @autobind errback(err) {
    if (err) this.emit('error', err)
  }

  erase(path, cb) {
    pump( this.readStream({ path, values: false })
        , through2.obj(function(key, _, next) {
            return next(null, { path: key[0], erased: true })
          })
        , batchStream(this)
        , cb )
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
      next: function outerNext(cb) {
        function innerNext(err, key, value) {
          if (err || key === undefined) return cb(err)

          if (!old) { // Skip same (older) paths
            if (pathEquals(key[0], prev)) {
              return iter.end(function innerEnd(err) {
                if (err) return cb(err)

                // Skip ahead to next path
                // TODO: this should be done natively with iterator#seek.
                range.gt = [prev, HI]
                return (iter = props.iterator(range)).next(innerNext)
              })
            }

            prev = key[0]
          }

          if (key[3] && !erased) return iter.next(innerNext)

          cb(null, paths ? key[0] : keys ? key : null, values ? value : null)
        }

        iter.next(innerNext)
      },

      end: function outerEnd(cb) {
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
  _writePatches(batch, newClock, patches, options = {}) {
       // For test purposes: if false, old updates are not ignored
    const filter = options.filter !== false
        , defSource = options.source || this.id
        , props = {}

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

      if (typeof ts !== 'number' || ts <= 0 || typeof source !== 'string') {
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
