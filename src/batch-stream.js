"use strict";

const through2 = require('through2')

module.exports = function levelBatchStream(db, opts = {}) {
  const { silent, size = 50, type, highWaterMark, ...batchOpts,  } = opts
  const batch = []

  let timeout, batching = false, scheduled = false;

  const stream = through2.obj({ highWaterMark }, function (op, _, next) {
    if (type && !op.type) op.type = type
    batch.push(op)

    if (!scheduled) {
      if (batch.length >= size) {
        scheduled = true
        timeout = setTimeout(flushBatch, 0)
      } else if (timeout == null) {
        scheduled = true
        timeout = setTimeout(flushBatch, 500)
      }
    }

    next()
  }, function flush(cb) {
    flushBatch(cb, true)
  })

  function flushBatch(cb, force) {
    scheduled = false

    if (timeout != null) {
      clearTimeout(timeout)
      timeout = null
    }

    if (cb) {
      if (!batch.length && !batching) return process.nextTick(cb)
      stream.once('batch-drain', cb)
    }

    if (batch.length && !batching) {
      batching = true
      const ops = batch.splice(0, batch.length)

      db.batch(ops, batchOpts, function(err) {
        batching = false

        if (err) {
          stream.emit('batch-error', err, ops)
          if (!silent) return stream.destroy(err)
        }

        if (!batch.length) {
          stream.emit('batch-drain')
        } else if (force){
          flushBatch(null, true)
        }
      })
    }
  }

  stream.flushBatch = function(cb) {
    flushBatch(cb, true)
  }

  return stream
}
