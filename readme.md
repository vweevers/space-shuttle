# space-shuttle

**Work in progress append-only scuttlebutt db with nested data.**

[![npm status](http://img.shields.io/npm/v/space-shuttle.svg?style=flat-square)](https://www.npmjs.org/package/space-shuttle) [![Travis build status](https://img.shields.io/travis/vweevers/space-shuttle.svg?style=flat-square&label=travis)](http://travis-ci.org/vweevers/space-shuttle) [![AppVeyor build status](https://img.shields.io/appveyor/ci/vweevers/space-shuttle.svg?style=flat-square&label=appveyor)](https://ci.appveyor.com/project/vweevers/space-shuttle) [![Dependency status](https://img.shields.io/david/vweevers/space-shuttle.svg?style=flat-square)](https://david-dm.org/vweevers/space-shuttle)

## features

- Nested data; construct object graphs of any size
- Retains full history. Data can't be truly deleted, but you can *erase* data. Erasing is like saying: "Forget that value I sent in an earlier update".
- Streaming replication with eventual consistency. The only thing it needs to keep in memory is the latest timestamp of each source (just to save unnecessary writes) (even without this, old updates will effectively be ignored because of how the db is ordered).
- Works with `leveldown` (node), `level.js` (browser) and `memdown` (any)
- ~~Compatible with [scuttlebutt/model](https://github.com/dominictarr/scuttlebutt)~~

## missing features

- Verification. You must control or be able to trust the peers. Currently, only the update *format* is validated (invalid updates are ignored). But a peer can send an update with a timestamp in the far future, which would invalidate all other future updates. It can also impersonate other peers, send updates on their behalf.
- Lists. You can write to `["a", 0]` but not read it out as an array.
- Sublevels/prefixes/cursors
- Behavior is undefined if you write values to `["a"]` and a sub-property `["a", "a"]`. One does not invalidate the other.
- Browsing a certain point in time. You can ask for "newer than x", but not "older than x". It's theoretically possible because `space-shuttle` saves data twice: newest-first (for object graphs) and oldest-first (for replication).
- Bytespace is the biggest bottleneck right now, maybe I'll switch to a handcoded thing with lexints etc

## example

*Example is out of date. I'm refactoring to move the drain event to the stream.*

```js
const space = require('space-shuttle')
    , assert = require('assert')
    , disk = require('test-level')({ clean: true })

const db1 = space('source a', disk())
    , db2 = space('source b', disk())

db1.batch([
  { path: ['penguins', 'henry', 'age'], value: 14 },
  { path: ['penguins', 'henry', 'hobby'], value: 'ice skating' }
], function(err) {
  // Setup live replication
  const s1 = db1.replicate()
      , s2 = db2.replicate()

  s2.pipe(s1).pipe(s2).once('sync', function(){
    // Construct a tree from the updated history
    db2.tree(['penguins'], function(err, tree){
      console.log('db2', tree)

      assert.deepEqual(tree, {
        henry: {
          age: 14,
          hobby: 'ice skating'
        }
      })

      // Note: without callbacks, you should add db.on('error', cb)
      db2.put(['penguins', 'henry', 'age'], 15)
      db2.put(['penguins', 'henry', 'likes'], ['emma', 'sunshine'])

      // Wait for db1 to have received and saved these updates
      db1.once('drain', function() {
        db1.tree(['penguins', 'henry'], function(err, tree){
          console.log('db1', tree)

          assert.deepEqual(tree, {
            age: 15,
            hobby: 'ice skating',
            likes: ['emma', 'sunshine']
          })
        })
      })
    })
  })
})
```

## install

With [npm](https://npmjs.org) do:

```
npm install space-shuttle
```

## license

[MIT](http://opensource.org/licenses/MIT) © Vincent Weevers
