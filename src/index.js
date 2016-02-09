"use strict";

const bytespace = require('bytespace')
    , bytewise = require('bytewise-core')
    , through2 = require('through2')
    , t = require('mini-type-assert')
    , Emitter = require('events').EventEmitter
    , timestamp = require('monotonic-timestamp')
    , kindOf = require('kindof')
    , eos = require('end-of-stream')
    , pump = require('pump')
    , pumpify = require('pumpify')
    , fastFuture = require('fast-future')
    , NotFoundError = require('level-errors').NotFoundError
    , probe = require('level-probe')
    , JSONStream = require('JSONStream')
    , duplexify = require('duplexify')
    , Stream = require('stream-wrapper')
    , autobind = require('autobind-decorator')
    , skipStream = require('./skip-stream')
    , batchStream = require('./batch-stream')
    , liveStream = require('./live-stream')

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

    this.id = id
    this.setMaxListeners(Number.MAX_VALUE)
    this.future = fastFuture()

    // Create root namespace
    this.db = bytespace(db, opts.ns || 'space-shuttle', {
      keyEncoding: bytewise, valueEncoding: 'json'
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

  // Duplex replication stream, adapted from and compatible with
  // dominictarr's scuttlebutt
  replicate(opts = {}) {
    const writable = opts.writable !== false // default to true
        , readable = opts.readable !== false
        , tail = opts.tail !== false
        , out = through2.obj({ allowHalfOpen: false })
        , d = duplexify.obj(null, out, { allowHalfOpen: false, destroy: true })

    d.name = opts.name

    let syncRecv = !writable
      , syncSent = !readable
      , ended = false
      , finished = false

    const sync = () => {
      d.emit('sync')
      if (!tail) this.future(d.dispose)
    }

    const sendHistory = (clock = {}) => {
      const opts = { compat: true, tail, clock }

      this.historyStream(opts).once('sync', () => {
        out.write(SYNC_SIGNAL)

        syncSent = true
        d.emit('syncSent')

        // When we have received remote's history
        if (syncRecv) sync()
      }).once('error', (err) => { d.dispose(err) }).pipe(out, { end: false })
    }

    d.setWritable(Stream.writable({ objectMode: true }, (data, _, next) => {
      if (Array.isArray(data)) { // It's an update
        if (writable && validate(data)) {
          // TODO: write test to simulate late batch, early stream end
          // TODO: write the initial burst of updates in batches
          return this.applyScuttlebuttUpdate(data, next)
        }
      } else if ('object' === typeof data && data) { // It's a digest
        if (syncSent) return next()
        else if (validate.digest(data)) sendHistory(data.clock)
        else return next(new Error('Invalid digest'))
      } else if (data === SYNC_SIGNAL) {
        syncRecv = true
        d.emit('syncReceived')
        if (syncSent) sync()
      }

      next()
    }))

    const start = () => {
      if (ended || finished || d.destroyed) return

      // Send my current clock so the other side knows what to send.
      // Clone the clock, because scuttlebutt mutates this object.
      const digest = { id: this.id, clock: {...this.memoryClock} }
      if (opts.meta) digest.meta = opts.meta

      if (readable) {
        out.write(digest)

        // What's this?
        if (!writable && !opts.clock) sendHistory()
      } else if (opts.sendClock) {
        out.write(digest)
      }
    }

    // Way too messy
    d.dispose = (err) => {
      this.removeListener('ready', start)
      this.removeListener('close', d.dispose)

      if (d.destroyed) return
      if (err) return d.destroy(err)
      if (!ended) out.end()

      // duplexify doesn't handle a duplex stream as readable well
      // or i'm doing something wrong
      if (finished && !ended) this.future(function(){
        const state = d._readable._readableState // streams2
        if (!d.destroyed && !ended && state.ended) {
          d.emit('end')
        }
      })
    }

    d.once('end', function() { ended = true; d.dispose() })
    d.once('finish', function() { finished = true; d.dispose() })
    this.on('close', d.dispose)

    if (this.ready) this.future(start)
    else this.once('ready', start)

    return d
  }

  // Compatibility with dc's scuttlebutt/model
  // TODO: support r-array (object trx)?
  applyScuttlebuttUpdate(update, cb) {
    const [ trx, ts, source ] = update
    const [ path, value ] = trx
    this.batch([{ source, ts, path, value }], cb)
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
      this.future(this._nextBatch)
    }
  }

  @autobind _nextBatch() {
    if (!this.ready) return this.once('ready', this._nextBatch)

    const [ patches, cb, options ] = this.queue.shift()

    this._writePatches(patches, options, (err) => {
      if (cb) cb(err); else if (err) this.emit('error', err)

      if (!this.queue.length) {
        this.writing = false
        this.emit('drain')
      } else {
        this._nextBatch()
      }
    })
  }

  erase(path, cb) {
    path = explode(path)

    const patches = [];
    const rs = this.readStream({ path, values: false }).on('data', (key) => {
      patches.push({ path: key[0], erased: true })
    })

    eos(rs, (err) => {
      if (!patches.length || err) cb(err)
      else this.batch(patches, cb)
    })
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

  readStream(opts = {}) {
    const { keys = true, values = true, path, old, erased, ...range } = opts

    if (path) {
      range.gt = [explode(path)]
      range.lt = [range.gt[0].concat(HI)]
    }

    let prev = [];

    return pump
      ( this.props.readStream({ keys: true, values, ...range }) // Always read keys
      , through2.obj(function(kv, _, next) {
          const key = values ? kv.key : kv

          if (!old) { // Skip same (older) paths (TODO: skip ahead)
            if (pathEquals(key[0], prev)) return next()
            else prev = key[0]
          }

          if (key[3] && !erased) next()
          else if (values && keys) next(null, kv)
          else if (keys) next(null, key)
          else next(null, kv.value)
        })
      )
  }

  // TODO: support patch.type = del for level* compatibility
  _writePatches(patches, options = {}, next) {
    const batch = []
        , props = {}
        , newClock = {}
       // For test purposes: if false, old updates are not ignored
        , filter = options.filter !== false
        , defSource = options.source || this.id

    patchLoop: for(let i=0, l=patches.length; i<l; i++) {
      const patch = patches[i]
        , { source = defSource, ts = timestamp(), value } = patch
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

    if (batch.length === 0) return this.future(next)

    // Update clocks to latest in batch
    Object.keys(newClock).forEach(source => {
      batch.push( { prefix: this.clock, key: source
                  , value: newClock[source] } )
    })

    this.db.batch(batch, next)
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
      Stream.writable({ objectMode: true }, function({ key, value }, _, next){
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
