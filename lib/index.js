"use strict";

var _slicedToArray = (function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i['return']) _i['return'](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError('Invalid attempt to destructure non-iterable instance'); } }; })();

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _createDecoratedClass = (function () { function defineProperties(target, descriptors, initializers) { for (var i = 0; i < descriptors.length; i++) { var descriptor = descriptors[i]; var decorators = descriptor.decorators; var key = descriptor.key; delete descriptor.key; delete descriptor.decorators; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor || descriptor.initializer) descriptor.writable = true; if (decorators) { for (var f = 0; f < decorators.length; f++) { var decorator = decorators[f]; if (typeof decorator === 'function') { descriptor = decorator(target, key, descriptor) || descriptor; } else { throw new TypeError('The decorator for method ' + descriptor.key + ' is of the invalid type ' + typeof decorator); } } if (descriptor.initializer !== undefined) { initializers[key] = descriptor; continue; } } Object.defineProperty(target, key, descriptor); } } return function (Constructor, protoProps, staticProps, protoInitializers, staticInitializers) { if (protoProps) defineProperties(Constructor.prototype, protoProps, protoInitializers); if (staticProps) defineProperties(Constructor, staticProps, staticInitializers); return Constructor; }; })();

var _get = function get(_x5, _x6, _x7) { var _again = true; _function: while (_again) { var object = _x5, property = _x6, receiver = _x7; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x5 = parent; _x6 = property; _x7 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var bytespace = require('bytespace'),
    bytewise = require('bytewise-core'),
    bytewiseHex = require('bytewise/encoding/hex'),
    through2 = require('through2'),
    t = require('mini-type-assert'),
    Emitter = require('events').EventEmitter,
    timestamp = require('monotonic-timestamp'),
    kindOf = require('kindof'),
    eos = require('end-of-stream'),
    pump = require('pump'),
    fastFuture = require('fast-future'),
    NotFoundError = require('level-errors').NotFoundError,
    probe = require('level-probe'),
    duplexify = require('duplexify'),
    writer = require('flush-write-stream'),
    autobind = require('autobind-decorator'),
    skipStream = require('./skip-stream'),
    batchStream = require('./batch-stream'),
    liveStream = require('./live-stream'),
    iteratorStream = require('level-iterator-stream'),
    noop = function noop() {};

var LO = bytewise.bound.lower(),
    HI = bytewise.bound.upper();

var SEP = '.',
    SYNC_SIGNAL = 'SYNC';

module.exports = function factory(id, db, opts) {
  return new SpaceShuttle(id, db, opts);
};

var SpaceShuttle = (function (_Emitter) {
  _inherits(SpaceShuttle, _Emitter);

  function SpaceShuttle(id, db) {
    var _this = this;

    var opts = arguments.length <= 2 || arguments[2] === undefined ? {} : arguments[2];

    _classCallCheck(this, SpaceShuttle);

    _get(Object.getPrototypeOf(SpaceShuttle.prototype), 'constructor', this).call(this);

    if (typeof id !== 'string' || !id) {
      throw new Error('A source ID is required');
    }

    // For test purposes (because proxyquireify broke browser tests)
    this.timestamp = opts.timestamp || timestamp;

    this.id = id;
    this.setMaxListeners(Number.MAX_VALUE);
    this.future = fastFuture();

    // Create root namespace
    this.db = bytespace(db, opts.ns || 'space-shuttle', {
      keyEncoding: bytewiseHex,
      valueEncoding: 'json',
      hexNamespace: true
    });

    // For traversal. Ordered by path, newest first, source:
    //  props<[path, negative_ts, source, erased], prop_value>
    this.props = this.db.sublevel('props');

    // For replication. Ordered by source, oldest first, path:
    //  inverse<[source, ts, path, erased], prop_value>
    this.inverse = this.db.sublevel('inverse');

    // For faster replication and ignoring old updates:
    //  clock<source, latest_ts>
    this.clock = this.db.sublevel('clock');

    // In-memory equivalent of this.clock
    this.memoryClock = Object.create(null);

    // Install a hook to insert and clean up inverse props
    this.props.pre(function (op, add) {
      var type = op.type;

      var _op$key = _slicedToArray(op.key, 4);

      var path = _op$key[0];
      var neg = _op$key[1];
      var source = _op$key[2];
      var erased = _op$key[3];
      var value = op.value;

      var inverseKey = [source, -neg, path, erased];

      if (type === 'del') {
        add({ prefix: _this.inverse, type: 'del', key: inverseKey });
      } else {
        add({ prefix: _this.inverse, type: 'put', key: inverseKey, value: value });
      }
    });

    this.queue = [];
    this.db.on('close', this.close);

    if (this.db.isOpen()) this.open();else this.db.once('open', this.open);
  }

  // Aliases

  _createDecoratedClass(SpaceShuttle, [{
    key: 'open',
    decorators: [autobind],
    value: function open() {
      var _this2 = this;

      // Read clock into memory
      var s = liveStream(this.clock).on('data', function (_ref) {
        var source = _ref.key;
        var ts = _ref.value;

        _this2.memoryClock[source] = ts;
      }).once('sync', function () {
        // Scuttlebutt streams and batches are deferred until now
        _this2.ready = true;
        _this2.emit('ready');
      }).once('error', this.emit.bind(this, 'error'));

      this.once('close', s.end.bind(s));
    }
  }, {
    key: 'close',
    decorators: [autobind],
    value: function close() {
      if (this.closed) return;
      if (this.queue.length) return this.once('drain', this.close);

      this.closed = true;
      this.emit('close');
    }

    // Read history since clock<source, ts>
  }, {
    key: 'historyStream',
    value: function historyStream(opts) {
      var _this3 = this;

      var _ref2 = opts || {};

      var _ref2$tail = _ref2.tail;
      var tail = _ref2$tail === undefined ? false : _ref2$tail;
      var _ref2$compat = _ref2.compat;
      var compat = _ref2$compat === undefined ? true : _ref2$compat;

      var clock = opts && opts.clock ? _extends({}, opts.clock) : {}; // Clone, b/c we mutate

      var output = through2.obj(function (_ref3, _, next) {
        var key = _ref3.key;
        var value = _ref3.value;

        var _key = _slicedToArray(key, 4);

        var source = _key[0];
        var ts = _key[1];
        var path = _key[2];
        var erased = _key[3];

        if (compat) {
          // Convert to dc's scuttlebutt format
          var trx = [join(path), erased ? undefined : value];
          next(null, [trx, ts, source]);
        } else {
          next(null, { source: source, ts: ts, path: path, erased: erased, value: value });
        }
      });

      // Stream sorted by timestamps, then sources
      var skipper = skipStream(this.inverse, this.clock, { clock: clock });

      eos(skipper, function (err) {
        if (err) return output.destroy(err);

        if (!tail) {
          output.emit('sync');
          return _this3.future(output.end.bind(output));
        }

        // Skip updates if remote already has them
        var filter = through2.obj(function (kv, _, next) {
          var _kv$key = _slicedToArray(kv.key, 2);

          var source = _kv$key[0];
          var ts = _kv$key[1];

          if (clock[source] && clock[source] >= ts) return next();else clock[source] = ts;
          next(null, kv);
        });

        pump(liveStream(_this3.inverse, { old: false, tail: true }), filter, output);

        output.emit('sync');
      });

      return skipper.pipe(output, { end: false });
    }
  }, {
    key: 'defer',
    value: function defer(onOpen, onClose) {
      var _this4 = this;

      var start = function start() {
        _this4.removeListener('close', cancel);
        _this4.future(onOpen);
      };

      var cancel = function cancel() {
        _this4.removeListener('ready', start);
        _this4.future(onClose);
      };

      this.once('ready', start);
      this.once('close', cancel);
    }

    // Duplex replication stream
  }, {
    key: 'replicate',
    value: function replicate(opts, _d) {
      var _this5 = this;

      if (opts === undefined) opts = {};

      var d = _d || duplexify.obj(null, null, { allowHalfOpen: true, destroy: false });
      d.name = opts.name;

      if (!this.ready) {
        this.defer(this.replicate.bind(this, opts, d), d.destroy.bind(d));
        return d;
      }

      var writable = opts.writable !== false,
          // default to true
      readable = opts.readable !== false,
          tail = opts.tail !== false;

      var syncRecv = !writable,
          syncSent = !readable;

      var sync = function sync() {
        d.emit('sync');
        if (!tail) _this5.future(d.dispose);
      };

      var sendHistory = function sendHistory(clock, cb) {
        var opts = { compat: true, tail: tail, clock: clock };

        d.setReadable(_this5.historyStream(opts).once('sync', function () {
          d.push(SYNC_SIGNAL);

          syncSent = true;
          d.emit('syncSent');

          // When we have received remote's history
          if (syncRecv) sync();
          if (cb) cb();
        }));
      };

      var batcher = batchStream(this);
      var incoming = through2.obj(function (data, _, next) {
        if (Array.isArray(data)) {
          // It's an update
          if (writable && validate(data)) {
            // TODO: write test to simulate late batch, early stream end
            var patch = _this5.convertScuttlebuttUpdate(data);
            return next(null, patch);
          }
        } else if ('object' === typeof data && data) {
          // It's a digest
          if (syncSent) return next();else if (validate.digest(data)) return sendHistory(data.clock, next);else return next(new Error('Invalid digest'));
        } else if (data === SYNC_SIGNAL) {
          syncRecv = true;
          d.emit('syncReceived');
          if (syncSent) sync();
        }

        next();
      }, function flush(cb) {
        eos(batcher, cb);
        process.nextTick(batcher.end.bind(batcher));
      });

      incoming.pipe(batcher);
      d.setWritable(incoming);

      var destroy = d.destroy;
      d.dispose = d.destroy = function (err) {
        _this5.removeListener('close', d.dispose);
        d.removeListener('finish', d.dispose);

        if (d.disposed) return;
        d.disposed = true;

        if (d._readable) d._readable.end();
        if (err) destroy.call(d, err);
      };

      d.on('finish', d.dispose);
      this.on('close', d.dispose);

      // Send my current clock so the other side knows what to send.
      // Clone the clock, because scuttlebutt mutates this object.
      var digest = { id: this.id, clock: _extends({}, this.memoryClock) };
      if (opts.meta) digest.meta = opts.meta;

      if (readable) {
        d.push(digest);

        // What's this?
        if (!writable && !opts.clock) sendHistory();
      } else if (opts.sendClock) {
        d.push(digest);
      }

      return d;
    }

    // Temporary compatibility with dc's scuttlebutt/model
  }, {
    key: 'convertScuttlebuttUpdate',
    value: function convertScuttlebuttUpdate(update) {
      var _update = _slicedToArray(update, 3);

      var trx = _update[0];
      var ts = _update[1];
      var source = _update[2];

      var _trx = _slicedToArray(trx, 2);

      var path = _trx[0];
      var value = _trx[1];

      return { source: source, ts: ts, path: path, value: value };
    }
  }, {
    key: 'sublevel',
    value: function sublevel(prefix) {
      // TODO
    }
  }, {
    key: 'put',
    value: function put(path, value, options, cb) {
      this.batch([{ path: path, value: value }], options, cb);
    }
  }, {
    key: 'batch',
    value: function batch(patches, options, cb) {
      if (typeof options === 'function') cb = options, options = {};

      this.queue.push([patches, cb, options]);

      if (!this.writing) {
        this.writing = true;
        this._nextBatch();
      }
    }

    // this is overengineered, but keeping it for now b/c tests rely on it
  }, {
    key: '_nextBatch',
    decorators: [autobind],
    value: function _nextBatch() {
      var _this6 = this;

      if (!this.ready) {
        return this.defer(this._nextBatch, function () {
          _this6.emit('error', new Error('premature close, pending batch'));
        });
      }

      var batch = [],
          props = {},
          newClock = {},
          callbacks = [];

      for (var i = 0, l = this.queue.length; i < l; i++) {
        var _queue$i = _slicedToArray(this.queue[i], 3);

        var patches = _queue$i[0];
        var cb = _queue$i[1];
        var options = _queue$i[2];

        if (cb) callbacks.push(cb);
        this._writePatches(batch, props, newClock, patches, options);
      }

      this.queue = [];

      var next = function next(err) {
        if (callbacks.length) callbacks.forEach(function (f) {
          return f(err);
        });else if (err) _this6.emit('error', err);

        if (!_this6.queue.length) {
          _this6.writing = false;
          if (batch.length > 0) _this6.emit('drain');
        } else {
          _this6.future(_this6._nextBatch);
        }
      };

      if (batch.length === 0) return this.future(next);

      // Update clocks to latest in batch. Add to beginning, so that
      // this.memoryClock is updated (with post hook)
      // before any open historystreams receive the updates
      var prepend = Object.keys(newClock).map(function (source) {
        return { prefix: _this6.clock, key: source, value: newClock[source] };
      });

      this.db.batch(prepend.concat(batch), next);
    }
  }, {
    key: 'erase',
    value: function erase(path, cb) {
      pump(this.readStream({ path: path, values: false }), through2.obj(function (key, _, next) {
        return next(null, { path: key[0], erased: true });
      }), batchStream(this), cb);
    }
  }, {
    key: 'get',
    value: function get(path, opts, cb) {
      if (typeof opts === 'function') cb = opts, opts = {};
      path = explode(path);

      // Ignore keys and values options
      var _opts = opts;
      var keys = _opts.keys;
      var values = _opts.values;
      var erased = _opts.erased;

      var range = _objectWithoutProperties(_opts, ['keys', 'values', 'erased']);

      // TODO: path.concat(HI)?
      range.gt = [path, LO];
      range.lt = [path, HI];

      probe(this.props, range, function (err, kv) {
        if (err && err.notFound) return cb(notFound(path));else if (err) return cb(err);

        if (kv.key[3] && !erased) cb(notFound(path));else cb(null, kv.value);
      });
    }
  }, {
    key: 'iterator',
    value: function iterator() {
      var opts = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];
      var _opts$keys = opts.keys;
      var keys = _opts$keys === undefined ? true : _opts$keys;
      var _opts$values = opts.values;
      var values = _opts$values === undefined ? true : _opts$values;
      var paths = opts.paths;
      var path = opts.path;
      var old = opts.old;
      var erased = opts.erased;

      var range = _objectWithoutProperties(opts, ['keys', 'values', 'paths', 'path', 'old', 'erased']);

      if (path) {
        range.gt = [explode(path)];
        range.lt = [range.gt[0].concat(HI)];
      }

      range.keys = true; // Always read keys
      range.values = values;

      var prev = [],
          props = this.props,
          iter = props.iterator(range);

      return {
        next: function next(cb) {
          var handle = function handle(err, key, value) {
            if (err || key === undefined) return cb(err);

            if (!old) {
              // Skip same (older) paths
              if (pathEquals(key[0], prev)) {
                iter.end(noop);

                // Skip ahead to next path
                // TODO: this should be done natively with iterator#seek.
                range.gt = [prev, HI];
                return (iter = props.iterator(range)).next(handle);
              }

              prev = key[0];
            }

            if (key[3] && !erased) return iter.next(handle);

            cb(null, paths ? key[0] : keys ? key : null, values ? value : null);
          };

          iter.next(handle);
        },

        end: function end(cb) {
          iter.end(cb);
        }
      };
    }
  }, {
    key: 'readStream',
    value: function readStream() {
      var opts = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];
      var _opts$keys2 = opts.keys;
      var keys = _opts$keys2 === undefined ? true : _opts$keys2;
      var _opts$values2 = opts.values;
      var values = _opts$values2 === undefined ? true : _opts$values2;

      var decoder = keys && values ? void 0 : keys ? keyOnly : valueOnly;
      return iteratorStream(this.iterator(opts), _extends({ decoder: decoder }, opts));
    }

    // TODO: support patch.type = del for level* compatibility
  }, {
    key: '_writePatches',
    value: function _writePatches(batch, props, newClock, patches) {
      var options = arguments.length <= 4 || arguments[4] === undefined ? {} : arguments[4];

      // For test purposes: if false, old updates are not ignored
      var filter = options.filter !== false,
          defSource = options.source || this.id;

      patchLoop: for (var i = 0, l = patches.length; i < l; i++) {
        var patch = patches[i];
        var _patch$source = patch.source;

        var _source = _patch$source === undefined ? defSource : _patch$source;

        var _patch$ts = patch.ts;
        var ts = _patch$ts === undefined ? this.timestamp() : _patch$ts;
        var value = patch.value;
        var path = explode(patch.path == null ? patch.key : patch.path);
        var erased = value == null || patch.erased === true;

        // Validate
        if (!Array.isArray(path) || path.length === 0) {
          // Emit a normalized patch
          this.emit('invalid', { path: path, ts: ts, source: _source, value: value, erased: erased });
          continue;
        }

        for (var _i = 0, _l = path.length; _i < _l; _i++) {
          var _t = typeof path[_i];

          if (path[_i] === '__proto__' || _t !== 'number' && _t !== 'string') {
            this.emit('invalid', { path: path, ts: ts, source: _source, value: value, erased: erased });
            continue patchLoop;
          }
        }

        if (typeof ts !== 'number' || typeof _source !== 'string') {
          this.emit('invalid', { path: path, ts: ts, source: _source, value: value, erased: erased });
          continue;
        }

        if (filter) {
          var id = path.join(SEP);

          // Ignore old and out of order updates
          if (props[id] > ts || this.memoryClock[_source] >= ts || newClock[_source] > ts) {
            this.emit('old', { path: path, ts: ts, source: _source, value: value, erased: erased });
            continue;
          }

          // this.memoryClock is updated by post hook
          props[id] = ts;
        }

        newClock[_source] = ts;

        batch.push({ prefix: this.props, key: [path, -ts, _source, erased],
          value: erased ? '' : value });
      }
    }
  }, {
    key: 'writeStream',
    value: function writeStream() {}
    // TODO: batchStream(this)

    // TODO: if ts of ancestor (latest ts in branch) is > ours, ignore child?

  }, {
    key: 'tree',
    value: function tree(path, done) {
      if (typeof path === 'function') done = path, path = [];

      var prefixLength = explode(path).length;
      var root = {};

      pump(this.readStream({ path: path }), writer.obj(function (_ref4, _, next) {
        var key = _ref4.key;
        var value = _ref4.value;

        var path = key[0].slice(prefixLength),
            node = root;

        if (!path.length) path.push('_');

        for (var i = 0, l = path.length; i < l; i++) {
          var seg = path[i],
              type = kindOf(node[seg]);

          if (i === path.length - 1) node[seg] = value;else if (type === 'undefined') node = node[seg] = {};else if (type !== 'object') node = node[seg] = { _: node[seg] };else node = node[seg];
        }

        next();
      }), function (err) {
        done(err, root);
      });
    }
  }]);

  return SpaceShuttle;
})(Emitter);

SpaceShuttle.prototype.createReadStream = SpaceShuttle.prototype.readStream;
SpaceShuttle.prototype.createWriteStream = SpaceShuttle.prototype.writeStream;
SpaceShuttle.prototype.createStream = SpaceShuttle.prototype.replicate;

SpaceShuttle.pathEquals = pathEquals;

function explode(path) {
  if (typeof path === 'number') return [path];else if (typeof path === 'string') return path.split(SEP);else return path;
}

function join(path) {
  if (typeof path === 'number') return '' + path;else if (typeof path === 'string') return path;else return path.join(SEP);
}

function pathEquals(a, b) {
  var l = a.length;
  if (l !== b.length) return false;
  for (; l--;) if (a[l] !== b[l]) return false;
  return true;
}

function notFound(k) {
  return new NotFoundError('Key not found in database [' + join(k) + ']');
}

function keyOnly(key) {
  return key;
}
function valueOnly(_, value) {
  return value;
}

// Taken from dc's scuttlebutt
function validate(data) {
  if (!(Array.isArray(data) && 'string' === typeof data[2] && '__proto__' !== data[2] // this would break stuff
   && 'number' === typeof data[1])) return false;

  return true;
}

validate.digest = function (digest) {
  return digest && typeof digest.clock === 'object' ? true : false;
};