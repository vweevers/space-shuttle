"use strict";

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
    merge2 = require('merge2'),
    noop = function noop() {};

var LO = bytewise.bound.lower(),
    HI = bytewise.bound.upper();

var SEP = '.',
    SYNC_SIGNAL = 'SYNC',
    READONLY_SIGNAL = 'READONLY';

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
    this.unhookPre = this.props.pre(function (op, add) {
      // Destructuring is nice but babel needlessly adds iterator support
      var path = op.key[0],
          neg = op.key[1],
          source = op.key[2],
          erased = op.key[3];

      var inverseKey = [source, -neg, path, erased];

      if (op.type === 'del') {
        add({ prefix: _this.inverse, type: 'del', key: inverseKey });
      } else {
        add({ prefix: _this.inverse, type: 'put', key: inverseKey, value: op.value });
      }
    });

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
      var update = function update(op) {
        if (op.type !== 'del') {
          _this2.memoryClock[op.key] = op.value; // clock<source, ts>
        }
      };

      this.clock.createReadStream().on('data', update).on('error', this.errback).on('end', function () {
        var unhook = _this2.clock.post(update);
        _this2.once('close', unhook);

        // Scuttlebutt streams and batches are deferred until now
        _this2.ready = true;
        _this2.emit('ready');
      });
    }
  }, {
    key: 'close',
    decorators: [autobind],
    value: function close() {
      if (!this.closed) {
        this.unhookPre();
        this.closed = true;
        this.emit('close');
      }
    }

    // Read history since clock<source, ts>
    // TODO: test that s.end() removes the hook
  }, {
    key: 'historyStream',
    value: function historyStream(opts) {
      var _this3 = this;

      var _ref = opts || {};

      var _ref$tail = _ref.tail;
      var tail = _ref$tail === undefined ? false : _ref$tail;
      var _ref$compat = _ref.compat;
      var compat = _ref$compat === undefined ? true : _ref$compat;

      var clock = opts && opts.clock ? _extends({}, opts.clock) : {}; // Clone, b/c we mutate

      var outer = through2.obj(function (_ref2, _, next) {
        var key = _ref2.key;
        var value = _ref2.value;

        var source = key[0],
            ts = key[1],
            path = key[2],
            erased = key[3];

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
      var streams = [skipper];

      eos(skipper, { readable: true, writable: false }, function (err) {
        if (err) outer.destroy(err);else outer.emit('sync');
      });

      if (tail) {
        var live = liveStream(this.inverse, { old: false, tail: true });

        // Skip updates if remote already has them
        streams.push(live.pipe(through2.obj(function filter(kv, _, next) {
          var source = kv.key[0],
              ts = kv.key[1];
          if (clock[source] && clock[source] >= ts) return next();else clock[source] = ts;
          next(null, kv);
        })));
      }

      return merge2.apply(undefined, streams.concat([{ objectMode: true, end: false }])).on('queueDrain', function () {
        return _this3.future(outer.end.bind(outer));
      }) // Delay end
      .pipe(outer);
    }
  }, {
    key: 'defer',
    value: function defer(onOpen, onClose) {
      var _this4 = this;

      var open = function open() {
        if (onClose) _this4.removeListener('close', close);
        _this4.future(onOpen);
      };

      var close = function close() {
        if (onOpen) _this4.removeListener('ready', open);
        _this4.future(onClose);
      };

      if (onOpen) this.once('ready', open);
      if (onClose) this.once('close', close);
    }

    // Duplex replication stream
  }, {
    key: 'replicate',
    value: function replicate(opts, _d) {
      var _this5 = this;

      if (opts === undefined) opts = {};

      var d = _d || duplexify.obj(null, null, { allowHalfOpen: true, destroy: false });
      d.name = opts.name || 'replicate_' + this.id;

      if (!this.ready) {
        this.defer(this.replicate.bind(this, opts, d), function () {
          d.destroy();
        });
        return d;
      }

      // Default to true
      var writable = opts.writable !== false,
          readable = opts.readable !== false,
          tail = opts.tail !== false;

      var syncRecv = false,
          syncSent = false;

      // We're readable and other side is writable
      var sendHistory = function sendHistory(clock, next) {
        var opts = { compat: true, tail: tail, clock: clock };

        d.setReadable(_this5.historyStream(opts).once('sync', function () {
          sync('send');
          next();
        }));
      };

      // We're not readable or other side is not writable
      var skipHistory = function skipHistory(isVoid, cb) {
        process.nextTick(function () {
          sync('send');

          // End if both sides are not writable
          if (isVoid || !tail) d.setReadable(null);else d.setReadable(through2.obj());

          cb && cb();
        });
      };

      // Handle signals and updates
      var batcher = batchStream(this, function (data, next) {
        if (Array.isArray(data)) {
          // It's an update
          if (!writable) next(new Error('Not writable'));else if (!validate(data)) next(new Error('Invalid update'));else {
            var patch = _this5.convertScuttlebuttUpdate(data);
            return _this5.filter(patch) ? next(null, patch) : next();
          }
        } else if ('object' === typeof data && data) {
          // It's a digest
          if (syncSent) next(new Error('Sync signal already sent'));else if (!readable) skipHistory(true, next);else if (validate.digest(data)) sendHistory(data.clock, next);else next(new Error('Invalid digest'));
        } else if (data === SYNC_SIGNAL) {
          if (syncRecv) next(new Error('Sync signal already received'));else sync('receive', next);
        } else if (data === READONLY_SIGNAL) {
          skipHistory(false, next);
        } else {
          next(new Error('Invalid data'));
        }
      });

      d.setWritable(batcher.on('commit', d.emit.bind(d, 'commit')));

      function sync(direction, cb) {
        if (direction === 'send') {
          d.push(SYNC_SIGNAL);
          syncSent = true;
          d.emit('syncSent');
        } else {
          syncRecv = true;
          d.emit('syncReceived');
        }

        if (syncSent && syncRecv) {
          batcher.commit(function () {
            // Fully consumed each other's history. Switch to tick batching
            batcher.setWindow(1);
            d.emit('sync');
          });
        }

        if (cb) cb();
      }

      var destroy = d.destroy;
      var onclose = d.end.bind(d);

      // Make sure the readable side ends
      var dispose = d.destroy = function (err) {
        _this5.removeListener('close', onclose);
        d.removeListener('finish', dispose);

        if (d.disposed) return;
        d.disposed = true;

        if (d._readable) d._readable.end();
        if (err) destroy.call(d, err);
      };

      d.on('finish', dispose);
      this.on('close', onclose);

      if (writable) {
        // Send my current clock so the other side knows what to send.
        var digest = { id: this.id, clock: _extends({}, this.memoryClock) };
        if (opts.meta) digest.meta = opts.meta;
        d.push(digest);
      } else {
        d.push(READONLY_SIGNAL);
      }

      return d;
    }
  }, {
    key: 'filter',
    value: function filter(patch) {
      if (!patch.source) throw new Error('Missing source on patch');
      if (!patch.ts) throw new Error('Missing timestamp on patch');

      if (this.memoryClock[patch.source] >= patch.ts) {
        this.emit('old', patch);
        return false;
      }

      return true;
    }

    // Temporary compatibility with dc's scuttlebutt/model
  }, {
    key: 'convertScuttlebuttUpdate',
    value: function convertScuttlebuttUpdate(update) {
      var path = update[0][0],
          value = update[0][1],
          ts = update[1],
          source = update[2];

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
      var _this6 = this;

      if (typeof options === 'function') cb = options, options = {};

      if (!this.ready) {
        return this.defer(function () {
          _this6.batch(patches, options, cb);
        });
      }

      var batch = [],
          newClock = {};
      this._writePatches(batch, newClock, patches, options);
      if (batch.length === 0) return cb && this.future(cb);

      // Update clocks to latest in batch. Add to beginning, so
      // that this.memoryClock is updated (with post hook)
      // before any open historyStreams receive the updates
      var prepend = Object.keys(newClock).map(function (source) {
        return { prefix: _this6.clock, key: source, value: newClock[source] };
      });

      this.db.batch(prepend.concat(batch), cb || this.errback);
    }
  }, {
    key: 'errback',
    decorators: [autobind],
    value: function errback(err) {
      if (err) this.emit('error', err);
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
        next: function outerNext(cb) {
          function innerNext(err, key, value) {
            if (err || key === undefined) return cb(err);

            if (!old) {
              // Skip same (older) paths
              if (pathEquals(key[0], prev)) {
                return iter.end(function innerEnd(err) {
                  if (err) return cb(err);

                  // Skip ahead to next path
                  // TODO: this should be done natively with iterator#seek.
                  range.gt = [prev, HI];
                  return (iter = props.iterator(range)).next(innerNext);
                });
              }

              prev = key[0];
            }

            if (key[3] && !erased) return iter.next(innerNext);

            cb(null, paths ? key[0] : keys ? key : null, values ? value : null);
          }

          iter.next(innerNext);
        },

        end: function outerEnd(cb) {
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
    value: function _writePatches(batch, newClock, patches) {
      var options = arguments.length <= 3 || arguments[3] === undefined ? {} : arguments[3];

      // For test purposes: if false, old updates are not ignored
      var filter = options.filter !== false,
          defSource = options.source || this.id,
          props = {};

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

        if (typeof ts !== 'number' || ts <= 0 || typeof _source !== 'string') {
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

      pump(this.readStream({ path: path }), writer.obj(function (_ref3, _, next) {
        var key = _ref3.key;
        var value = _ref3.value;

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