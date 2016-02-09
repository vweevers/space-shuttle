const pumpify = require('pumpify')
    , JSONStream = require('JSONStream')

const inherit = ['name', 'meta', 'id']
const fwd = ['syncReceived', 'syncRecieved', 'sync', 'syncSent', 'synced', 'header']

// Because scuttlebutt's stream-serializer is incompatible with JSONStream,
// disable stream-serializer and wrap with JSONStream
// TODO: add to space-shuttle and patch scuttlebutt prototype
function createStream(model, opts) {
  let { wrapper, wrap, ...rest } = opts || {}
  wrapper = wrapper || wrap

  rest.wrapper = 'raw'
  const stream = model.createStream(rest)

  if (wrapper === 'json') {
    return wrapStream(stream, { fwd, inherit })
  } else {
    return stream
  }
}

function wrapStream(streams, opts) {
  const { fwd = [], inherit = [] } = opts || {}

  streams = [].concat(streams)
  const inner = streams[0]

  streams.unshift(JSONStream.parse())
  streams.push(JSONStream.stringify(false))

  const outer = pumpify(streams)

  if (inner) {
    // Forward non-stream events
    fwd.forEach(function(event) {
      inner.on(event, function(data) {
        outer.emit(event, data)
      })
    })

    inherit.forEach(function(p) {
      if (inner[p] != null) outer[p] = inner[p]
    })
  }

  return outer
}

module.exports = wrapStream
module.exports.createStream = createStream
