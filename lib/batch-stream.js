"use strict";

var writer = require('flush-write-stream');

module.exports = function BatchStream(db, opts, map) {
  if (typeof opts === 'function') map = opts, opts = {};else if (!opts) opts = {};

  var batching = false,
      scheduled = false,
      draining = false,
      finished = false;
  var window_ = opts.window && opts.window > 0 ? opts.window : 50;

  // Using flush-write-stream instead of through2, because we
  // need an asynchronous flush on the writable side (before
  // finish is emitted) instead of on the readable side.
  var highWaterMark = opts.highWaterMark || 16;
  var w = writer.obj({ highWaterMark: highWaterMark }, map ? mapped : write, flush);
  var batch = [];

  function mapped(data, enc, next) {
    map(data, function callback(err, op) {
      if (err) next(err);else if (op === null) next(null, null);else write(op, enc, next);
    });
  }

  function write(op, enc, next) {
    if (op) batch.push(op);
    if (batch.length < window_) return next();

    w.once('commit', next);
    if (!scheduled) schedule();
  }

  function schedule() {
    scheduled = true;
    if (!batching) process.nextTick(commit);
  }

  function flush(cb) {
    commit(function done() {
      finished = true;
      cb();
    });
  }

  function commit(cb) {
    if (finished || w.destroyed) return cb && cb();

    if (cb) {
      if (!batch.length && !batching) return cb();
      draining = true;
      w.once('commit', cb);
    }

    if (!batching) {
      batching = true;
      db.batch(batch.splice(0, batch.length), opts, function post(err) {
        if (err) return w.destroy(err);

        batching = false;

        if (!batch.length) {
          draining = false;
          scheduled = false;
          w.emit('commit');
        } else if (draining || scheduled) {
          commit();
        }
      });
    }
  }

  w.commit = function forceCommit(cb) {
    if (!cb) throw new Error('Missing callback');
    commit(cb);
  };

  w.setWindow = function setWindow(n) {
    window_ = n && n > 0 ? n : 1;
    if (!scheduled && batch.length >= window_) schedule();
  };

  return w;
};