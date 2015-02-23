var R = require('ramda')
var P = require('bluebird')
var axios = require('axios')

var queues = require('./queues')

var servers = [
  {
    url: 'xxx',
    queuePointers: [{
      queueName: 'queueA',
      lastCounter: 4
    }]
  }
]

// TODO: new servers won't have queuePointers, so don't update them until we
// get a response
function poll (server) {
  var syncData = queues.diffAll(server.queuePointers)
  var url = server.url + '/sync'
  axios.post(url, syncData)
}

function sync (res, idx) {
  servers[idx].queuePointers = res.queuePointers
  queues.sync(res.syncData)
}

function pollAll () {
  var promises = R.map(poll, servers)
  P.map(promises, sync)
}

setInterval(pollAll, 1000)
