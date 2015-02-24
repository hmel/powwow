var path = require('path')
var describe = require('mocha').describe
var before = require('mocha').before
var it = require('mocha').it
var assert = require('assert')

var queue = require('../../lib/queue')

describe('Queue', function () {
  before(function () {
    queue.setRoot(path.resolve(__dirname, '../fixtures/queues'))
  })

  it('should load a queue', function () {
    return queue.load()
    .then(function () {
      assert.equal(queue.get('test1', 0).foo, 'bar')
      assert.equal(queue.get('test1', 1).baz, 'qux')
      assert.equal(queue.get('test2', 0).take.me, 'out')
      assert.equal(queue.get('test2', 1).to, 'the ballgame')
    })
  })
})
