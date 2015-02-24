/* global define */

var queues = require('../../../lib/queues')

define(function (require) {
  var registerSuite = require('intern!object')

  registerSuite({
    'passing test': function () {},
    'failing test': function () {
      throw new Error('Oops')
    }
  })
})
