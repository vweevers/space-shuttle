"use strict";

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

var through2 = require('through2');

module.exports = function levelBatchStream(db) {
  var opts = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];
  var silent = opts.silent;
  var _opts$size = opts.size;
  var size = _opts$size === undefined ? 50 : _opts$size;
  var type = opts.type;
  var highWaterMark = opts.highWaterMark;

  var batchOpts = _objectWithoutProperties(opts, ['silent', 'size', 'type', 'highWaterMark']);

  var batch = [];

  var timeout = undefined,
      batching = false,
      scheduled = false;

  var stream = through2.obj({ highWaterMark: highWaterMark }, function (op, _, next) {
    if (type && !op.type) op.type = type;
    batch.push(op);

    if (!scheduled) {
      if (batch.length >= size) {
        scheduled = true;
        timeout = setTimeout(flushBatch, 0);
      } else if (timeout == null) {
        scheduled = true;
        timeout = setTimeout(flushBatch, 500);
      }
    }

    next();
  }, function flush(cb) {
    flushBatch(cb, true);
  });

  function flushBatch(cb, force) {
    scheduled = false;

    if (timeout != null) {
      clearTimeout(timeout);
      timeout = null;
    }

    if (cb) {
      if (!batch.length && !batching) return process.nextTick(cb);
      stream.once('batch-drain', cb);
    }

    if (batch.length && !batching) {
      (function () {
        batching = true;
        var ops = batch.splice(0, batch.length);

        db.batch(ops, batchOpts, function (err) {
          batching = false;

          if (err) {
            stream.emit('batch-error', err, ops);
            if (!silent) return stream.destroy(err);
          }

          if (!batch.length) {
            stream.emit('batch-drain');
          } else if (force) {
            flushBatch(null, true);
          }
        });
      })();
    }
  }

  stream.flushBatch = function (cb) {
    flushBatch(cb, true);
  };

  return stream;
};