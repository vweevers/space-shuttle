'use strict';

function _objectWithoutProperties(obj, keys) { var target = {}; for (var i in obj) { if (keys.indexOf(i) >= 0) continue; if (!Object.prototype.hasOwnProperty.call(obj, i)) continue; target[i] = obj[i]; } return target; }

var pumpify = require('pumpify'),
    JSONStream = require('JSONStream');

var inherit = ['name', 'meta', 'id'];
var fwd = ['syncReceived', 'syncRecieved', 'sync', 'syncSent', 'synced', 'header'];

// Because scuttlebutt's stream-serializer is incompatible with JSONStream,
// disable stream-serializer and wrap with JSONStream
// TODO: add to space-shuttle and patch scuttlebutt prototype
function createStream(model, opts) {
  var _ref = opts || {};

  var wrapper = _ref.wrapper;
  var wrap = _ref.wrap;

  var rest = _objectWithoutProperties(_ref, ['wrapper', 'wrap']);

  wrapper = wrapper || wrap;

  rest.wrapper = 'raw';
  var stream = model.createStream(rest);

  if (wrapper === 'json') {
    return wrapStream(stream, { fwd: fwd, inherit: inherit });
  } else {
    return stream;
  }
}

function wrapStream(streams, opts) {
  var _ref2 = opts || {};

  var _ref2$fwd = _ref2.fwd;
  var fwd = _ref2$fwd === undefined ? [] : _ref2$fwd;
  var _ref2$inherit = _ref2.inherit;
  var inherit = _ref2$inherit === undefined ? [] : _ref2$inherit;

  streams = [].concat(streams);
  var inner = streams[0];

  streams.unshift(JSONStream.parse());
  streams.push(JSONStream.stringify(false));

  var outer = pumpify(streams);

  if (inner) {
    // Forward non-stream events
    fwd.forEach(function (event) {
      inner.on(event, function (data) {
        outer.emit(event, data);
      });
    });

    inherit.forEach(function (p) {
      if (inner[p] != null) outer[p] = inner[p];
    });
  }

  return outer;
}

module.exports = wrapStream;
module.exports.createStream = createStream;