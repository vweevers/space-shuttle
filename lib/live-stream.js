'use strict';

var future = require('fast-future')(),
    through2 = require('through2'),
    eos = require('end-of-stream');

// Like level-live-stream, but that wouldn't work with bytespace
// (though it should work now that a bug with post hooks has been fixed)
function liveStream(db) {
  var opts = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

  var old = opts.old !== false,
      tail = opts.tail !== false,
      output = through2.obj();

  var live = function live(err) {
    if (err) return output.destroy(err);

    if (tail) {
      (function () {
        var stop = function stop() {
          if (unhook) unhook();
          unhook = null;
        };

        var unhook = db.post(function (op) {
          if (op.type !== 'del') output.write(op);
        });

        output.once('error', stop);
        output.once('close', stop);
        output.once('finish', stop);
        output.once('end', stop);
      })();
    }

    output.emit('sync');
    if (!tail) future(output.end.bind(output));
  };

  if (old) {
    var rs = db.createReadStream();
    eos(rs, { writable: false }, live);
    rs.pipe(output, { end: false });
  } else {
    live();
  }

  return output;
}

module.exports = liveStream;