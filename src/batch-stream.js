"use strict";

const through2 = require('through2')

module.exports = function levelBatchStream(db, opts = {}) {
  const { silent, type, highWaterMark, ...batchOpts,  } = opts
  const batch = []

  let timeout;

  const stream = through2.obj({ highWaterMark }, function (op, _, next) {
    if (type && !op.type) op.type = type
    batch.push(op)

    if (timeout == null) {
      timeout = setTimeout(function(){
        timeout = null
        flushBatch()
      }, 500)
    }

    next()
  }, function flush(cb) {
    clearTimeout(timeout)
    timeout = null
    flushBatch(cb)
  })

  function flushBatch(cb) {
    if (batch.length) {
      const ops = batch.splice(0, batch.length)

      db.batch(ops, batchOpts, function(err) {
        if (err) {
          stream.emit('batch-error', err, ops)
          if (!silent) return stream.destroy(err)
        }

        cb && cb()
      })
    } else {
      cb && cb()
    }
  }

  return stream
}
