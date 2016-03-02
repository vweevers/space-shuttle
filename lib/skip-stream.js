'use strict';

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var iterator = require('level-iterator'),
    bytewise = require('bytewise-core'),
    sorted = require('sorted'),
    Readable = require('readable-stream').Readable,
    noop = function noop() {};

var LO = bytewise.bound.lower(),
    HI = bytewise.bound.upper();

// A stream sorted by timestamps, then sources.
//
// Does a heap-sorted skip scan on
//   db<[source, ts, path, erased], prop_value>
//
// TODO: external merge sort
function skipStream(db, clockDb, opts) {
  var _opts$clock = opts.clock;
  var clock = _opts$clock === undefined ? {} : _opts$clock;
  var _opts$highWaterMark = opts.highWaterMark;
  var highWaterMark = _opts$highWaterMark === undefined ? 16 : _opts$highWaterMark;

  var distinct = [],
      heap = sorted([], cmp),
      stream = PullReadable.obj({ highWaterMark: highWaterMark }, pull);

  // Could collect distinct sources on `db` using
  // `level-probe`, but this is faster.
  clockDb.keyStream().on('data', function (key) {
    distinct.push(key);
  }).on('end', iterate).on('error', stream.destroy.bind(stream));

  return stream;

  // On each leading edge (source), iterate predicate edge range (ts)
  function iterate() {
    if (!distinct.length) return stream.push(null);

    distinct.forEach(function (source) {
      var ts = clock[source] || LO;
      next(db.iterator({ gt: [source, ts, HI], lt: [source, HI] }));
    });
  }

  function next(iter) {
    iter.next(function innerNext(err, key, value) {
      if (err) return stream.destroy(err);

      if (key === undefined) {
        return iter.end(function innerEnd(err) {
          if (err) return stream.destroy(err);

          if (--distinct.length === 0) {
            // Flush and end
            heap.forEach(function (entry) {
              return stream.push(entry[1]);
            });
            stream.push(null);
          } else if (heap.length) {
            stream.pull();
          }
        });
      }

      // Sorting on key[1], which is "ts", the predicate edge
      heap.push([key[1], { key: key, value: value }, iter]);

      // Write when we have an element from each iterator
      if (heap.length === distinct.length) stream.pull();
    });
  }

  function pull() {
    // Take the first (smallest) element from the heap, push kv, then refill
    var entry = heap.shift();
    var kv = entry[1],
        iter = entry[2];
    next(iter);
    return kv;
  }
}

module.exports = skipStream;

function cmp(a_, b_) {
  var a = a_[0],
      b = b_[0];

  if (a === b) return 0;else if (a > b) return 1;else if (a < b) return -1;else throw new RangeError('Unstable comparison: ' + a + ' cmp ' + b);
}

var PullReadable = (function (_Readable) {
  _inherits(PullReadable, _Readable);

  _createClass(PullReadable, null, [{
    key: 'obj',
    value: function obj(opts, pull) {
      if (typeof opts === 'function') pull = opts, opts = {};
      opts = _extends({ highWaterMark: 16 }, opts, { objectMode: true });
      return new PullReadable(opts, pull);
    }
  }]);

  function PullReadable(opts, pull) {
    _classCallCheck(this, PullReadable);

    if (typeof opts === 'function') pull = opts, opts = {};
    _get(Object.getPrototypeOf(PullReadable.prototype), 'constructor', this).call(this, opts);

    this.destroyed = false;

    this._reading = false;
    this._pendingPull = false;
    this._pull = pull;
  }

  // Consumer must call pull()

  _createClass(PullReadable, [{
    key: 'pull',
    value: function pull() {
      if (this.destroyed) return;

      if (this._reading) {
        this._pendingPull = false;

        var chunk = this._pull();

        if (chunk === undefined) return;else if (chunk === null) this.push(chunk);else if (!this.push(chunk)) this._reading = false;
      } else {
        this._pendingPull = true;
      }
    }
  }, {
    key: '_read',
    value: function _read() {
      if (this._reading || this.destroyed) return;
      this._reading = true;
      if (this._pendingPull) this.pull();
    }
  }, {
    key: 'destroy',
    value: function destroy(err) {
      var _this = this;

      if (this.destroyed) return;
      this.destroyed = true;

      process.nextTick(function () {
        if (err) _this.emit('error', err);
        _this.emit('close');
      });
    }
  }]);

  return PullReadable;
})(Readable);