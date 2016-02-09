const tape = require('tape')
    , mem = require('test-level')({ mem: true, clean: true })
    , disk = require('test-level')('space-shuttle/*', { clean: true })
    , same = require('./util/collect').same
    , after = require('after')
    , eos = require('end-of-stream')
    , Model = require('scuttlebutt/model')
    , createStream = require('../json-wrap').createStream
    , proxyquire = require('proxyquire')
    , timestamp = require('monotonic-timestamp')
    , timestampSpy = []
    , space = proxyquire('../', {
        'monotonic-timestamp': function() {
          const ts = timestamp()
          timestampSpy.push(ts)
          timestampSpy.splice(100, timestampSpy.length) // trim
          return ts
        }
    })

run('memdown', mem)
run('leveldown', disk)

function run(adapter, factory) {
  const test = function(name, opts, cb) {
    if (typeof opts === 'function') cb = opts, opts = {}

    tape(`${adapter} :: ${name}`, opts, function(t, ...args){
      const test = t.test

      t.test = function subtest(name, opts, cb) {
        if (typeof opts === 'function') cb = opts, opts = {}
        test.call(t, `${adapter} :: ${name}`, opts, cb)
      }

      cb(t, ...args)
    })
  }

  test('basic', (t) => {
    const db = space('test-id', factory())
    const value = 'spacer'

    t.plan(15)

    db.put('a', value, (err) => {
      t.ifError(err, 'no error')

      const path = ['a']
      const ts = timestampSpy.pop()

      t.is(typeof ts, 'number', 'got ts')

      same( t, db.props
          , [ { key: [path, -ts, 'test-id', false], value } ], 'prop ok')

      same( t, db.inverse
          , [ { key: ['test-id', ts, path, false], value } ], 'inverse ok')

      same( t, db.clock
          , [ { key: 'test-id', value: ts } ], 'clock ok')

      db.erase('a', function(err){
        t.ifError(err, 'no erase error')

        db.get('a', { erased: true }, function(err, value) {
          t.ifError(err, 'no get error')
          t.same(value, '')
        })

        db.get('a', function(err, value) {
          t.ok(err, 'got error')
        })

        const newTs = timestampSpy.pop()
        t.isNot(newTs, ts, 'new ts')

        same( t, db.clock
            , [ { key: 'test-id', value: newTs  } ], 'clock ok')
      })
    })
  })

  test('erase sub prop', (t) => {
    const db = space('test-id', factory())

    t.plan(11)

    db.batch([
      { key: 'a',     value: 'a'     },
      { key: 'b',     value: 'b'     },
      { key: 'b.b',   value: 'b.b'   },
      { key: 'b.c',   value: 'b.c'   },
      { key: 'b.c.d', value: 'b.c.d' },
      { key: 'c',     value: 'c'     }
    ], function(err){
      t.ifError(err, 'no batch err')
      const ts = timestampSpy.splice(-6, 6)
      const lengthBefore = timestampSpy.length

      same( t, db.props, [
        { key: [ [ 'a' ], -ts[0], 'test-id', false ], value: 'a' },
        { key: [ [ 'b' ], -ts[1], 'test-id', false ], value: 'b' },
        { key: [ [ 'b', 'b' ], -ts[2], 'test-id', false ], value: 'b.b' },
        { key: [ [ 'b', 'c' ], -ts[3], 'test-id', false ], value: 'b.c' },
        { key: [ [ 'b', 'c', 'd' ], -ts[4], 'test-id', false ], value: 'b.c.d' },
        { key: [ [ 'c' ], -ts[5], 'test-id', false ], value: 'c' }
      ], 'props ok', function(){

        db.erase('b', function(err){
          t.ifError(err, 'no erase err')

          t.is(timestampSpy.length-lengthBefore, 4, 'added 4')
          const delTs = timestampSpy.splice(-4, 4)

          same( t, db.props, [
            { key: [ [ 'a' ], -ts[0], 'test-id', false ], value: 'a' },
            { key: [ [ 'b' ], -delTs[0], 'test-id', true ], value: '' },
            { key: [ [ 'b' ], -ts[1], 'test-id', false ], value: 'b' },
            { key: [ [ 'b', 'b' ], -delTs[1], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'b' ], -ts[2], 'test-id', false ], value: 'b.b' },
            { key: [ [ 'b', 'c' ], -delTs[2], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'c' ], -ts[3], 'test-id', false ], value: 'b.c' },
            { key: [ [ 'b', 'c', 'd' ], -delTs[3], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'c', 'd' ], -ts[4], 'test-id', false ], value: 'b.c.d' },
            { key: [ [ 'c' ], -ts[5], 'test-id', false ], value: 'c' }
          ], 'props ok after erase')

          same( t, db, [
            { key: [ [ 'a' ], -ts[0], 'test-id', false ], value: 'a' },
            { key: [ [ 'c' ], -ts[5], 'test-id', false ], value: 'c' }
          ], 'readstream without opts.erased ok')

          same( t, db, { erased: true }, [
            { key: [ [ 'a' ], -ts[0], 'test-id', false ], value: 'a' },
            { key: [ [ 'b' ], -delTs[0], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'b' ], -delTs[1], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'c' ], -delTs[2], 'test-id', true ], value: '' },
            { key: [ [ 'b', 'c', 'd' ], -delTs[3], 'test-id', true ], value: '' },
            { key: [ [ 'c' ], -ts[5], 'test-id', false ], value: 'c' }
          ], 'readstream with opts.erased ok')
        })
      })
    })
  })

  test('double', (t) => {
    const db = space('test-id', factory())
    const value = 3.14159

    t.plan(6)

    db.put('a', value, (err) => {
      t.ifError(err, 'no error')

      db.get('a', function(err, value) {
        t.ifError(err, 'no get error')
        t.is(value, 3.14159)
      })
    })

    db.put('b', Number.MIN_VALUE, (err) => {
      t.ifError(err, 'no error')

      db.get('b', function(err, value) {
        t.ifError(err, 'no get error')
        t.is(value, Number.MIN_VALUE)
      })
    })
  })

  test('boolean', (t) => {
    const db = space('test-id', factory())

    t.plan(6)

    db.put('b', false, (err) => {
      t.ifError(err, 'no error')

      db.put('a', true, (err) => {
        t.ifError(err, 'no error')

        db.get('b', function(err, value) {
          t.ifError(err, 'no get error')
          t.is(value, false)
        })

        db.get('a', function(err, value) {
          t.ifError(err, 'no get error')
          t.is(value, true)
        })
      })
    })
  })

  test('batch order', { }, (t) => {
    const db = space('test-id', factory())
    const newest = timestamp()*2

    t.plan(21)

    db.batch([
      { path: 'x.y', value: 'last path' },
      { key: 'a', value: true },
      { key: 'aa', value: false, source: 'boOP' },
      { path: 'c.d', value: 12 },
      { path: ['c', 'e'], value: 0 },
      { path: ['c', 'e'], value: 2 },
      { path: 0, value: 'newest first path', ts: newest }, // simulate out of order
      { path: 0, value: 'older first path' },
      { path: 0, value: 'newer first path' }
    ], { source: 'beep', filter: false }, function(err){
      t.ifError(err, 'no batch err')

      const ts = timestampSpy.splice(-8, 8)

      // ordered by path, newest, source
      same( t, db.props, [
        { key: [ [0] , -newest, 'beep', false ], value: 'newest first path' },
        { key: [ [0] , -ts[7], 'beep', false ], value: 'newer first path' },
        { key: [ [0] , -ts[6], 'beep', false ], value: 'older first path' },
        { key: [ ['a'] , -ts[1], 'beep', false ], value: true },
        { key: [ ['aa'] , -ts[2], 'boOP', false ], value: false },
        { key: [ ['c', 'd'] , -ts[3], 'beep', false ], value: 12 },
        { key: [ ['c', 'e'] , -ts[5], 'beep', false ], value: 2 }, // new first
        { key: [ ['c', 'e'] , -ts[4], 'beep', false ], value: 0 },
        { key: [ ['x', 'y'] , -ts[0], 'beep', false ], value: 'last path' }
      ], 'props ok' )

      // ordered by source, oldest, path
      same( t, db.inverse, [
        { key: [ 'beep', ts[0], ['x', 'y'], false ], value: 'last path' },
        { key: [ 'beep', ts[1], ['a'], false ], value: true },
        { key: [ 'beep', ts[3], ['c', 'd'], false ], value: 12 },
        { key: [ 'beep', ts[4], ['c', 'e'], false ], value: 0 }, // old first
        { key: [ 'beep', ts[5], ['c', 'e'], false ], value: 2 },
        { key: [ 'beep', ts[6], [0], false ], value: 'older first path' },
        { key: [ 'beep', ts[7], [0], false ], value: 'newer first path' },
        { key: [ 'beep', newest, [0], false ], value: 'newest first path' },
        { key: [ 'boOP', ts[2], ['aa'] , false ], value: false },
      ], 'inverse ok' )

      // ordered by oldest, source
      same(t, db.historyStream(), [
        [ [ 'x.y', 'last path' ], ts[0], 'beep' ],
        [ [ 'a', true ], ts[1], 'beep' ],
        [ [ 'aa', false ], ts[2], 'boOP' ],
        [ [ 'c.d', 12 ], ts[3], 'beep' ],
        [ [ 'c.e', 0 ], ts[4], 'beep' ],
        [ [ 'c.e', 2 ], ts[5], 'beep' ],
        [ [ '0', 'older first path' ], ts[6], 'beep' ],
        [ [ '0', 'newer first path' ], ts[7], 'beep' ],
        [ [ '0', 'newest first path' ], newest, 'beep' ]
      ], 'compat history ok' )

      same(t, db.historyStream({ compat: false }), [
        { erased: false, path: [ 'x', 'y' ], value: 'last path', ts: ts[0], source: 'beep' },
        { erased: false, path: [ 'a'      ], value: true, ts: ts[1], source: 'beep' },
        { erased: false, path: [ 'aa'     ], value: false, ts: ts[2], source: 'boOP' },
        { erased: false, path: [ 'c', 'd' ], value: 12, ts: ts[3], source: 'beep' },
        { erased: false, path: [ 'c', 'e' ], value: 0, ts: ts[4], source: 'beep' },
        { erased: false, path: [ 'c', 'e' ], value: 2, ts: ts[5], source: 'beep' },
        { erased: false, path: [ 0        ], value: 'older first path', ts: ts[6], source: 'beep' },
        { erased: false, path: [ 0        ], value: 'newer first path', ts: ts[7], source: 'beep' },
        { erased: false, path: [ 0        ], value: 'newest first path', ts: newest, source: 'beep' }
      ], 'history ok' )

      same(t, db.historyStream({ clock: { boOP: 0, beep: ts[4] } }), [
        [ [ 'aa', false ], ts[2], 'boOP' ],
        [ [ 'c.e', 2 ], ts[5], 'beep' ],
        [ [ '0', 'older first path' ], ts[6], 'beep' ],
        [ [ '0', 'newer first path' ], ts[7], 'beep' ],
        [ [ '0', 'newest first path' ], newest, 'beep' ]
      ], 'history since clock ok (1)' )

      same(t, db.historyStream({ clock: { boOP: ts[2], beep: ts[4] } }), [
        [ [ 'c.e', 2 ], ts[5], 'beep' ],
        [ [ '0', 'older first path' ], ts[6], 'beep' ],
        [ [ '0', 'newer first path' ], ts[7], 'beep' ],
        [ [ '0', 'newest first path' ], newest, 'beep' ]
      ], 'history since clock ok (2)' )

      same(t, db.historyStream({ clock: { beep: ts[7] } }), [
        [ [ 'aa', false ], ts[2], 'boOP' ],
        [ [ '0', 'newest first path' ], newest, 'beep' ]
      ], 'history since clock ok (3)' )

      same(t, db.historyStream({ clock: { beep: newest } }), [
        [ [ 'aa', false ], ts[2], 'boOP' ],
      ], 'history since clock ok (4)' )

      same(t, db.historyStream({ clock: { beep: newest, boOP: ts[2] } }), [
      ], 'history since latest clock is empty' )

      same(t, db.historyStream({ clock: {} }), [
        [ [ 'x.y', 'last path' ], ts[0], 'beep' ],
        [ [ 'a', true ], ts[1], 'beep' ],
        [ [ 'aa', false ], ts[2], 'boOP' ],
        [ [ 'c.d', 12 ], ts[3], 'beep' ],
        [ [ 'c.e', 0 ], ts[4], 'beep' ],
        [ [ 'c.e', 2 ], ts[5], 'beep' ],
        [ [ '0', 'older first path' ], ts[6], 'beep' ],
        [ [ '0', 'newer first path' ], ts[7], 'beep' ],
        [ [ '0', 'newest first path' ], newest, 'beep' ]
      ], 'history since empty clock is full' )
    })
  })

  test('live history', (t) => {
    const db = space('one', factory())
    const ignoredTs = 1

    t.plan(8)

    db.batch([
      { key: 'a', value: true },
      { key: 'a.a', value: false, source: 'two' }
    ], function(err){
      t.ifError(err, 'no batch err')

      const ts = timestampSpy.splice(-2, 2)
      const stream = db.historyStream({ tail: true, clock: { ignored: ignoredTs } })

      const acc = []
      stream.on('data', (data) => acc.push(data))

      stream.on('sync', () => {
        t.ok(true, 'emits sync')

        t.same(acc, [
          [ [ 'a', true ], ts[0], 'one' ],
          [ [ 'a.a', false ], ts[1], 'two' ]
        ], 'live history after sync ok')

        db.batch([
          { path: 'b', value: 12 },
          { path: ['c', 'e'], value: 2 },
          { path: 'x', value: 'beep', source: 'ignored', ts: ignoredTs }
        ], function(err){
          t.ifError(err, 'no batch err')

          const ts2 = timestampSpy.splice(-2, 2)

          eos(stream, function(err){
            t.ifError(err, 'no err')

            t.same(acc, [
              [ [ 'a', true ], ts[0], 'one' ],
              [ [ 'a.a', false ], ts[1], 'two' ],
              [ [ 'b', 12 ], ts2[0], 'one' ],
              [ [ 'c.e', 2 ], ts2[1], 'one' ]
            ], 'live history ok')

            same(t, db.historyStream(), [
              [ [ 'x', 'beep' ], ignoredTs, 'ignored' ],
              [ [ 'a', true ], ts[0], 'one' ],
              [ [ 'a.a', false ], ts[1], 'two' ],
              [ [ 'b', 12 ], ts2[0], 'one' ],
              [ [ 'c.e', 2 ], ts2[1], 'one' ]
            ], 'non-live history ok')
          })

          stream.end()
        })
      })
    })
  })

  test('replicate', (t) => {
    t.test('replicate: basic events in objectMode', (t) => {
      const db1 = space('a', factory())
      const db2 = space('b', factory())

      t.plan(8)

      const stream1 = db1.replicate({ wrapper: 'raw', tail: false })
      const stream2 = db2.replicate({ wrapper: 'raw', tail: false })

      stream1.on('sync', () => t.ok(true, 's1 emits sync'))
      stream1.on('syncReceived', () => t.ok(true, 's1 emits syncReceived'))
      stream1.on('syncSent', () => t.ok(true, 's1 emits syncSent'))

      stream2.on('sync', () => t.ok(true, 's2 emits sync'))
      stream2.on('syncReceived', () => t.ok(true, 's2 emits syncReceived'))
      stream2.on('syncSent', () => t.ok(true, 's2 emits syncSent'))

      stream1.pipe(stream2).pipe(stream1)

      eos(stream1, function(err) { t.ifError(err, 'no s1 error') })
      eos(stream2, function(err) { t.ifError(err, 'no s2 error') })
    })

    t.test('replicate: basic events in json mode', (t) => {
      const db1 = space('a', factory())
      const db2 = space('b', factory())

      t.plan(8)

      const stream1 = createStream(db1, { tail: false })
      const stream2 = createStream(db2, { tail: false })

      stream1.on('sync', () => t.ok(true, 's1 emits sync'))
      stream1.on('syncReceived', () => t.ok(true, 's1 emits syncReceived'))
      stream1.on('syncSent', () => t.ok(true, 's1 emits syncSent'))

      stream2.on('sync', () => t.ok(true, 's2 emits sync'))
      stream2.on('syncReceived', () => t.ok(true, 's2 emits syncReceived'))
      stream2.on('syncSent', () => t.ok(true, 's2 emits syncSent'))

      stream1.pipe(stream2).pipe(stream1)

      eos(stream1, function(err) { t.ifError(err, 'no s1 error') })
      eos(stream2, function(err) { t.ifError(err, 'no s2 error') })
    })

    t.test('replicate: from a to b', (t) => {
      const db1 = space('a', factory())
      const db2 = space('b', factory())

      t.plan(5)

      db1.batch([
        { key: 'a', value: 1 },
        { key: 'b.b', value: 'beee' }
      ], function(err) {
        t.ifError(err, 'no batch err')

        const ts = timestampSpy.splice(-2, 2)

        const stream1 = db1.replicate({ tail: false })
        const stream2 = db2.replicate({ tail: false })

        stream1.pipe(stream2).pipe(stream1)

        eos(stream1, function(err) { t.ifError(err, 'no s1 error') })
        eos(stream2, function(err) {
          t.ifError(err, 'no s2 error')

          same(t, db2.readStream(), [
            { key: [ [ 'a' ], -ts[0], 'a', false ], value: 1 },
            { key: [ [ 'b', 'b' ], -ts[1], 'a', false ], value: 'beee' }
          ], 'data ok')
        })
      })
    })

    t.test('replicate: live', (t) => {
      const db1 = space('a', factory())
      const db2 = space('b', factory())

      t.plan(12)

      const stream1 = db1.replicate()
      const stream2 = db2.replicate()

      stream1.pipe(stream2).pipe(stream1)

      const next = after(4, function(err){
        t.ifError(err, 'no after error')

        const ts = timestampSpy.splice(-2, 2)

        same(t, db1.readStream(), [
          { key: [ [ 'a' ], -ts[0], 'a', false ], value: 1 },
          { key: [ [ 'b' ], -ts[1], 'b', false ], value: 2 }
        ], 'data db1 ok')

        same(t, db2.readStream(), [
          { key: [ [ 'a' ], -ts[0], 'a', false ], value: 1 },
          { key: [ [ 'b' ], -ts[1], 'b', false ], value: 2 }
        ], 'data db2 ok')

        stream1.once('end', () => t.ok(true, 's1.end called'))
        stream1.once('finish', () => t.ok(true, 's1.finish called'))
        // stream1.once('close', () => t.ok(true, 's1.close called'))

        stream2.once('end', () => t.ok(true, 's2.end called'))
        stream2.once('finish', () => t.ok(true, 's2.finish called'))
        // stream2.once('close', () => t.ok(true, 's2.close called'))

        eos(stream1, function(err) { t.ifError(err, 'no s1 end error') })
        eos(stream2, function(err) { t.ifError(err, 'no s2 end error') })

        stream2.dispose()
      })

      // Should fire twice: for local put and remote update
      db1.on('drain', next)
      db2.on('drain', next)

      stream1.once('sync', function() {
        t.ok(true, 'emits sync')

        db1.put('a', 1)
        db2.put('b', 2)
      })
    })
  })

  test('replicate to scuttlebutt (objectMode)', (t) => {
    const db = space('test-id', factory())
    const model = Model('beep')

    t.plan(4)

    const s1 = db.createStream({ wrapper: 'raw' })
        , s2 = model.createStream({ wrapper: 'raw' })

    s2.pipe(s1).pipe(s2).on('sync', function(){
      t.ok(true, 'emits sync')

      model.set('a', { beep: 'boop' })
      db.on('drain', function() {
        t.ok(true, 'drained')

        db.tree(function(err, tree){
          t.ifError(err, 'no error')
          t.same(tree, {
            a: { beep: 'boop' }
          })
        })
      })
    })
  })

  test('replicate to scuttlebutt (json)', (t) => {
    const db = space('test-id', factory())
    const model = Model('beep')

    t.plan(4)

    const s1 = createStream(db, { tail: true, wrapper: 'json' })
        , s2 = createStream(model, { tail: true, wrapper: 'json' })

    s2.pipe(s1).pipe(s2).on('sync', function(){
      t.ok(true, 'emits sync')

      model.set('a', { beep: 'boop' })
      db.on('drain', function() {
        t.ok(true, 'drained')

        db.tree(function(err, tree){
          t.ifError(err, 'no error')
          t.same(tree, {
            a: { beep: 'boop' }
          })
        })
      })
    })
  })

  test('tree', (t) => {
    const db = space('test-id', factory())

    t.plan(8)

    const expected = {
      _: 'b',
      b: 'b.b',
      c: {
        _: 'b.c',
        d: 'b.c.d'
      },
      d: 'b.d',
      e: {
        _: 'b.e',
        foo: 'b.e.foo'
      }
    }

    db.batch([
      { key: 'a',         value: 'a'       },
      { key: 'b.e.foo',   value: 'b.e.foo' },
      { key: 'b.e',       value: 'b.e'     },
      { key: 'b',         value: 'b'       },
      { key: 'b.b',       value: 'b.b'     },
      { key: 'b.c',       value: 'b.c'     },
      { key: 'b.c.d',     value: 'b.c.d'   },
      { key: 'b.d',       value: 'b.d'     },
      { key: 'c',         value: 'c'       }
    ], function(err){
      t.ifError(err, 'no batch err')
      const ts = timestampSpy.splice(-6, 6)

      db.tree('b', function(err, res){
        t.ifError(err, 'no tree err')

        t.same(res, expected, 'tree1 ok')

        db.batch([
          { key: 'b.x.y',   value: 'b.x.y'   },
          { key: 'b.z.y.w', value: 'b.z.y.w' },
          { key: 'b.z.w', value: 'b.z.w' }
        ], function(err){
          t.ifError(err, 'no batch err')

          db.tree('b', function(err, res){
            t.ifError(err, 'no tree err')

            expected.x = { y: 'b.x.y' }
            expected.z = { y: { w: 'b.z.y.w' }, w: 'b.z.w' }

            t.same(res, expected, 'tree2 ok')

            db.tree(function(err, res){
              t.ifError(err, 'no tree err')

              t.same(res, { a: 'a', b: expected, c: 'c' }, 'tree3 ok')
            })
          })
        })
      })
    })
  })
}
