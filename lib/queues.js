var P = require('bluebird')
var fs = P.promisifyAll(require('fs'))
var R = require('ramda')
var E = require('error')

var queue = require('queue')

var root = 'data' // DEBUG

/*

TODO:
- Handle exceptions
- Tests

*/

function loadQueues (queueNames) {
  return P.all(R.map(queue.load, queueNames))
}

function load () {
  function stripDat (str) {
    return str.substr(0, str.length - 4)
  }

  return fs.readdirAsync(root)
  .then(function (files) {
    return loadQueues(R.map(stripDat, files))
  })
}

function diff (lastCounters) {
  return R.map(queue.diff, lastCounters)
}

function sync (recs) {
  R.forEach(queue.sync, recs)
}

module.exports = {
  load: load,
  sync: sync,
  diff: diff
}

//  TODO:
//    separate own queues from remote queues (two different hashes or hash of hashes?)
//      need to figure out how to organize queues into own and remote
//      can we put them all in one queue but keep a hash indicating own/remote?
//    subscribe: subscribe to remote queues
