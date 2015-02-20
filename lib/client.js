// var P = require('bluebird')

var Path = require('path')
var Bacon = require('baconjs')
var fs = require('fs')

var root = 'data' // DEBUG
var queues = {}
var persistQueues = {}
var locks = {}
/*
var s = Bacon.fromPoll(1000, poller)

function poller () {
  var rand = Math.random()
  if (rand < 0.5) return new Bacon.Next()
  return new Bacon.Next(rand)
}

s.filter(function (n) { return !!n}).log()
*/

function publish (queueName, payload) {
  var queue = queues[queueName]
  if (!queue) queue = queues[queueName] = []
  var rec = {
    counter: queue.length,
    payload: payload
  }
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
    console.log('DEBUG:')
    console.log(queue)
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

exports.publish = publish
exports.load = load
