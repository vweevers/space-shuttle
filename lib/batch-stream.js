"use strict";

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

var through2 = require('through2'),
    BatchStream = require('batch-stream'),
    duplexify = require('duplexify');

module.exports = function levelBatchStream(db) {
  var opts = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];
  var silent = opts.silent;
  var _opts$size = opts.size;
  var size = _opts$size === undefined ? 100 : _opts$size;
  var type = opts.type;

  var batchOpts = _objectWithoutProperties(opts, ['silent', 'size', 'type']);

  var ws = new BatchStream({ size: size });

  var rs = through2.obj(function (ops, _, next) {
    doBatch(ops, function (err) {
      if (err) {
        dup.emit('batch-error', err, ops);
        if (!silent) return dup.destroy(err);
      }

      next();
    });
  });

  function doBatch(ops, next) {
    if (type) {
      ops = ops.map(function (op) {
        op.type = op.type || type;
        return op;
      });
    }

    db.batch(ops, batchOpts, next);
  }

  ws.pipe(rs);

  // TODO: use pumpify
  var dup = duplexify.obj(ws, rs);

  dup.clearBatch = function () {
    return ws.batch.splice(0, ws.batch.length);
  };

  dup.getBatch = function () {
    return ws.batch.slice();
  };

  // TODO: include ops buffered in rs?
  dup.flushBatch = function (cb) {
    var batch = dup.clearBatch();
    doBatch(batch, cb);
  };

  return dup;
};