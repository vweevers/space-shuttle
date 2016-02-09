const space = require('./')
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
