const core = require('bytewise-core')

exports.encode = function (source) {
  return core.encode(source).toString('hex')
}

exports.decode = function (buffer) {
  return core.decode(buffer.toString('hex'))
}

exports.buffer = false
exports.type = 'bytewise-hex'
