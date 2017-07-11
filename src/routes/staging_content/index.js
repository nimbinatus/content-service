/* jshint node: true, esversion: 6*/

'use strict';

exports.store = require('./store').handler;
exports.bulk = require('./bulk').handler;
exports.retrieve = require('./retrieve').handler;
exports.check = require('./check').handler;
exports.list = require('./list').handler;
exports.remove = require('./remove').handler;
