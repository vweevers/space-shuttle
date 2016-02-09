"use strict";

const through2 = require('through2')
    , BatchStream = require('batch-stream')
    , duplexify = require('duplexify')

module.exports = function levelBatchStream(db, opts = {}) {
  const { silent, size = 100, type, ...batchOpts } = opts

  const ws = new BatchStream({ size })

  const rs = through2.obj(function(ops, _, next) {
    doBatch(ops, function(err) {
      if (err) {
        dup.emit('batch-error', err, ops)
        if (!silent) return dup.destroy(err)
      }

      next()
    })
  })

  function doBatch(ops, next) {
    if (type) {
      ops = ops.map(function(op){
        op.type = op.type || type
        return op
      })
    }

    db.batch(ops, batchOpts, next)
  }

  ws.pipe(rs)

  // TODO: use pumpify
  var dup = duplexify.obj(ws, rs)

  dup.clearBatch = function() {
    return ws.batch.splice(0, ws.batch.length)
  }

  dup.getBatch = function() {
    return ws.batch.slice()
  }

  // TODO: include ops buffered in rs?
  dup.flushBatch = function(cb) {
    const batch = dup.clearBatch()
    doBatch(batch, cb)
  }

  return dup
}
