const future = require('fast-future')()
    , through2 = require('through2')
    , eos = require('end-of-stream')

// Like level-live-stream, but that wouldn't work with bytespace
// (though it should work now that a bug with post hooks has been fixed)
function liveStream(db, opts = {}) {
  const old = opts.old !== false
      , tail = opts.tail !== false
      , output = through2.obj()

  const live = function(err) {
    if (err) return output.destroy(err)

    if (tail) {
      let unhook = db.post(function(op){
        if (op.type !== 'del') output.push(op)
      })

      function stop() {
        if (unhook) unhook()
        unhook = null
      }

      output.once('error', stop)
      output.once('close', stop)
      output.once('finish', stop)
      output.once('end', stop)
    }

    output.emit('sync')
    if (!tail) future(output.end.bind(output))
  }

  if (old) {
    const rs = db.createReadStream()
    eos(rs, { writable: false }, live)
    rs.pipe(output, { end: false })
  } else {
    live()
  }

  return output
}

module.exports = liveStream
