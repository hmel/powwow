// var P = require('bluebird')

var Path = require('path')
var fs = require('fs')
var R = require('ramda')
var E = require('error')

var root = 'data' // DEBUG
var queues = {}
var persistQueues = {}
var locks = {}

/*

TODO:
- Handle exceptions
- Tests

*/

function publish (queueName, payload) {
  var queue = queues[queueName]
  if (!queue) queue = queues[queueName] = []
  var rec = {
    counter: queue.length,
    payload: payload
  }
  save(queueName, rec)
}

function save (queueName, rec) {
  var queue = queues[queueName]
  if (!queue) queue = queues[queueName] = []
  queue.push(rec)
  persist(queueName, rec)
}

function load (queueName, cb) {
  if (queues[queueName]) throw new Error('Can\'t load non-empty queue: ' + queueName)
  var queue = queues[queueName] = []
  fs.readFile(filename(queueName), {encoding: 'utf8'}, function (err, data) {
    if (err) throw err
    var lines = data.split('\n')
    lines.forEach(function (line) {
      if (line.length !== 0) queue.push(JSON.parse(line))
    })
    cb()
  })
}

function filename (queueName) {
  return Path.join(root, queueName + '.dat')
}

function flush (queueName) {
  if (locks[queueName]) return
  locks[queueName] = true
  var persistQueue = persistQueues[queueName]
  if (persistQueue.length === 0) return
  var recStrings = persistQueue.map(JSON.stringify).join('\n')
  persistQueues[queueName] = []
  fs.appendFile(filename(queueName), recStrings + '\n', function(err) {
    if (err) throw err
    locks[queueName] = false
    flush(queueName)
  })
}

function persist (queueName, rec) {
  var persistQueue = persistQueues[queueName]
  if (!persistQueue) persistQueue = persistQueues[queueName] = []
  persistQueues[queueName].push(rec)
  flush(queueName)
}

// Return all new records since ``lastCounter``
function diffOne (queuePtr) {
  var queueName = queuePtr.queueName
  var lastCounter = queuePtr.lastCounter
  var queue = queues[queueName]
  if (!queue && lastCounter > 0) throw new E.NoSuchQueueError(queueName)
  if (lastCounter >= queue.length) throw new RangeError()
  return {
    queueName: queueName,
    diff: queue.slice(lastCounter + 1)
  }
}

function diff (lastCounters) {
  return R.map(diffOne, lastCounters)
}

// Save new records locally
// TODO: emit new records events for subscribers
function syncOne (syncData) {
  var queueName = syncData.queueName
  var recs = syncData.diff
  if (recs.length === 0) return
  var queue = queues[queueName]
  R.forEach(function (rec) {
    if (rec.counter <= queue.length) return
    if (rec.counter - queue.length !== 1) throw new E.GapError()
    save(queueName, rec)
  }, recs)
}

function sync (recs) {
  R.forEach(syncOne, recs)
}

module.exports = {
  publish: publish,
  load: load,
  sync: sync,
  diff: diff
}

//  TODO:
//    separate own queues from remote queues (two different hashes or hash of hashes?)
//      need to figure out how to organize queues into own and remote
//      can we put them all in one queue but keep a hash indicating own/remote?
//    poll: poll remote servers, sync incoming records, and provide diffs
//    provide Bacon streams for new records
//    subscribe: subscribe to remote queues
