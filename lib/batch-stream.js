"use strict";

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

var through2 = require('through2');

module.exports = function levelBatchStream(db) {
  var opts = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];
  var silent = opts.silent;
  var type = opts.type;
  var highWaterMark = opts.highWaterMark;

  var batchOpts = _objectWithoutProperties(opts, ['silent', 'type', 'highWaterMark']);

  var batch = [];

  var timeout = undefined;

  var stream = through2.obj({ highWaterMark: highWaterMark }, function (op, _, next) {
    if (type && !op.type) op.type = type;
    batch.push(op);

    if (timeout == null) {
      timeout = setTimeout(function () {
        timeout = null;
        flushBatch();
      }, 500);
    }

    next();
  }, function flush(cb) {
    clearTimeout(timeout);
    timeout = null;
    flushBatch(cb);
  });

  function flushBatch(cb) {
    if (batch.length) {
      (function () {
        var ops = batch.splice(0, batch.length);

        db.batch(ops, batchOpts, function (err) {
          if (err) {
            stream.emit('batch-error', err, ops);
            if (!silent) return stream.destroy(err);
          }

          cb && cb();
        });
      })();
    } else {
      cb && cb();
    }
  }

  return stream;
};