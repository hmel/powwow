var R = require('ramda')

/*global Promise:true*/
var Promise = require('bluebird')

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
  return axios.post(url, syncData)
  .then(function (res) {
    // TODO: check for 304
    server.queuePointers = res.queuePointers
    queues.sync(res.syncData)
  })
}

function pollAll () {
  return Promise.all(R.map(poll, servers))
}

setInterval(pollAll, 1000)
