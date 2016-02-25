const test = require('tape')
    , eos = require('end-of-stream')
    , profiler = require('v8-profiler')
    , fs = require('fs')
    , space = require('../')

const disk = require('test-level')('space-shuttle/*', { clean: true })
const TIME = Date.now()

test('replicate a lot', (t) => {
  const db1 = space('a', disk())
  const db2 = space('b', disk())

  var max = 1e5, count = 0

  profiler.startProfiling()
  insert()

  function insert() {
    const batch = []
    for(let i=0; i<100; i++) {
      batch.push({ key: count, value: count++ })
    }

    db1.batch(batch, function(err) {
      if (err) throw err
      if (count % 1e4 === 0) console.log('inserted %d', count)

      if (count<max) insert()
      else sync()
    })
  }

  function sync() {
    saveProfile('batch_put', function(err){
      if (err) throw err

      profiler.startProfiling()

      const stream1 = db1.replicate({ tail: false })
      const stream2 = db2.replicate({ tail: false })

      stream1.pipe(stream2).pipe(stream1)

      eos(stream2, function(err){
        t.ifError(err, 'no error from eos')

        saveProfile('sync', function(err){
          if (err) throw err
          t.end()
        })
      })
    })
  }

  function saveProfile(name, cb) {
    var cpuProfile = profiler.stopProfiling()
    var path = `debugdump/${TIME}-${name}.cpuprofile`

    cpuProfile.export()
      .pipe(fs.createWriteStream(path))
      .on('error', cb)
      .on('finish', function() {
        cpuProfile.delete();
        console.log('wrote profile to %s', path)
        cb()
      })
  }
})
