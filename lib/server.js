var Hapi = require('hapi')

var queue = require('./queue')

var server = new Hapi.Server()
server.connection({port: 3111})

server.start(function () {
  console.log('Server running at:', server.info.uri)
})

server.route({
  method: 'PATCH',
  path: '/sync',
  handler: sync
})

function sync (request, reply) {
  queue.sync(request.payload)
  var queuePointers = queue.queuePointers()
  if (queuePointers.length === 0) {
    reply(304)
  } else {
    reply(queuePointers)
  }
}
