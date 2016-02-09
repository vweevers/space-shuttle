const eos = require('end-of-stream')

module.exports = function collect(db, opts, cb) {
  if (typeof opts === 'function') cb = opts, opts = void 0

  const stream = db._readableState ? db : db.createReadStream(opts)
  const acc = []

  eos(stream.on('data', acc.push.bind(acc)), function(err){
    cb(err, acc)
  })
}

module.exports.same = function(t, db, opts, expected, msg, cb) {
  if (Array.isArray(opts)) cb = msg, msg = expected, expected = opts, opts = void 0
  if (typeof msg === 'function') cb = msg, msg = void 0

  module.exports(db, opts, function(err, pairs){
    t.ifError(err, 'no error')
    t.same(pairs, expected, msg)
    if (cb) cb(err, pairs)
  })
}
