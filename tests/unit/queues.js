var path = require('path')

/*global Promise:true*/
var Promise = require('bluebird')

var fs = Promise.promisifyAll(require('fs-extra'))
var R = require('ramda')

var describe = require('mocha').describe
var beforeEach = require('mocha').beforeEach
var it = require('mocha').it
var assert = require('assert')

var E = require('../../lib/error')
var queue = require('../../lib/queue')

describe('Queue', function () {
  beforeEach(function () {
    var fixtures = path.resolve(__dirname, '../fixtures/queues')
    return fs.ensureDirAsync('/tmp/queues')
    .then(function () {
      return fs.removeAsync('/tmp/queues')
    })
    .then(function () {
      return fs.copyAsync(fixtures, '/tmp/queues')
    })
    .then(function () {
      queue.config({root: '/tmp/queues', machineId: '1'})
      queue.clear()
      return queue.load()
    })
  })

  it('should have loaded the queue', function () {
    assert.equal(queue.get('1-test1', 0).foo, 'bar')
    assert.equal(queue.get('1-test1', 1).baz, 'qux')
    assert.equal(queue.get('2-test2', 0).take.me, 'out')
    assert.equal(queue.get('2-test2', 1).to, 'the ballgame')
  })

  it('should have diff', function () {
    var lastCounters = [
      {queueName: '1-test1', lastCounter: 1},
      {queueName: '2-test2', lastCounter: 0}
    ]
    var res = queue.diff(lastCounters)
    assert.deepEqual(res, [
      {queueName: '2-test2', diff: [
        {i: 1, rec: {to: 'the ballgame'}}
      ]}
    ])
  })

  it('should return correct value on new queue diff', function () {
    var lastCounters = [
      {queueName: '1-test1', lastCounter: 1},
      {queueName: '2-test2', lastCounter: null}
    ]
    var res = queue.diff(lastCounters)
    assert.deepEqual(res, [
      {queueName: '2-test2', diff: [
        {i: 0, rec: {take: {me: 'out'}}},
        {i: 1, rec: {to: 'the ballgame'}}
      ]}
    ])
  })

  it('should return empty list when up to date', function () {
    var lastCounters = [
      {queueName: '1-test1', lastCounter: 1},
      {queueName: '2-test2', lastCounter: 1}
    ]
    var res = queue.diff(lastCounters)
    assert.deepEqual(res, [])
  })

  it('should successfully publish a record', function () {
    var rec = {buy: 'me', some: 'peanuts'}
    queue.publish('1-test1', rec)
    assert.equal(queue.get('1-test1', 2).buy, 'me')

    // Wait until file is saved
    return Promise.delay(10)
    .then(function () {
      queue.clear()
      return queue.load()
    })
    .then(function () {
      assert.equal(queue.get('1-test1', 2).buy, 'me')
    })
  })

  it('should not publish to queues it doesn\'t own', function () {
    var rec = {buy: 'me', some: 'peanuts'}
    assert.throws(R.partial(queue.publish, '2-test2', rec), E.ReadOnlyError)
  })
})
