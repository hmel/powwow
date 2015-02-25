'use strict'

var E = module.exports = function generateError (name) {
  var CustomErr = function (msg) {
    this.message = msg
    this.name = name
    Error.captureStackTrace(this, CustomErr)
  }
  CustomErr.prototype = Object.create(Error.prototype)
  CustomErr.prototype.constructor = CustomErr

  return CustomErr
}

function addError (errorName) {
  module.exports[errorName] = E(errorName)
}

function addErrors (errors) {
  errors.forEach(addError)
}

addErrors([
  'NoSuchQueueError',
  'GapError',
  'ReadOnlyError'
])
