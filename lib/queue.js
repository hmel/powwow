var Path = require('path')

/*global Promise:true*/
var Promise = require('bluebird')

var fs = Promise.promisifyAll(require('fs'))
var R = require('ramda')

var E = require('./error')

var root = 'data' // DEBUG
var queues = {}
var persistQueues = {}
var locks = {}
var machineId

/*

TODO:
- Handle exceptions
- Tests

*/

function config (cfg) {
  machineId = cfg.machineId
  root = cfg.root
}

function publish (queueName, payload) {
  var queueMachineId = queueName.split('-')[0]
  if (queueMachineId !== machineId) {
    throw new E.ReadOnlyError('Can only publish to own queues: ' + queueName)
  }
  var queue = queues[queueName]
  if (!queue) queue = queues[queueName] = []
  var rec = {
    i: queue.length,
    rec: payload
  }
  save(queueName, rec)
}

function save (queueName, rec) {
  var queue = queues[queueName]
  if (!queue) queue = queues[queueName] = []
  queue.push(rec)
  persist(queueName, rec)
}

function loadOne (queueName, cb) {
  if (queues[queueName]) throw new Error('Can\'t load non-empty queue: ' + queueName)
  var queue = queues[queueName] = []
  return fs.readFileAsync(filename(queueName), {encoding: 'utf8'})
  .then(function (res) {
    var lines = res.split('\n')
    lines.forEach(function (line) {
      if (line.length !== 0) queue.push(JSON.parse(line))
    })
  })
}

function loadQueues (queueNames) {
  return Promise.all(R.map(loadOne, queueNames))
}

function clear () {
  queues = {}
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

function get (queueName, index) {
  return queues[queueName][index].rec
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
  fs.appendFile(filename(queueName), recStrings + '\n', function (err) {
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
  var start = R.isNil(lastCounter) ? 0 : lastCounter + 1
  var queue = queues[queueName]
  if (!queue && lastCounter > 0) throw new E.NoSuchQueueError(queueName)
  if (lastCounter >= queue.length) throw new RangeError()
  return {
    queueName: queueName,
    diff: queue.slice(start)
  }
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

function diff (lastCounters) {
  var emptyDiff = R.compose(R.isEmpty, R.prop('diff'))
  return R.reject(emptyDiff, R.map(diffOne, lastCounters))
}

function sync (recs) {
  R.forEach(syncOne, recs)
}

module.exports = {
  config: config,
  clear: clear,
  publish: publish,
  load: load,
  sync: sync,
  diff: diff,
  get: get
}

//  TODO:
//    separate own queues from remote queues (two different hashes or hash of hashes?)
//      need to figure out how to organize queues into own and remote
//      can we put them all in one queue but keep a hash indicating own/remote?
//    poll: poll remote servers, sync incoming records, and provide diffs
//    provide Bacon streams for new records
//    subscribe: subscribe to remote queues
