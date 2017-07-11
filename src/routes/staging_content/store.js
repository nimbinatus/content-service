/* jshint node: true, esversion: 6*/

'use strict';

const async = require('async');
const storage = require('../../storage');

/**
 * @description Store new content into the content service.
 */
exports.handler = function (req, res, next) {
  const contentID = req.params.id;
  const envelope = req.body;

  req.logger.debug('Content storage request received.', { contentID });

  storeStagingEnvelope(contentID, envelope, (err) => {
    if (err) {
      req.logger.reportError('Unable to store content.', err);
      return next(err);
    }

    res.send(204);
    req.logger.reportSuccess('Content storage successful.', { statusCode: 204, contentID });
    next();
  });
};

const storeStagingEnvelope = exports.storeStagingEnvelope = function (contentID, envelope, callback) {
  async.parallel([
    (cb) => storage.storeStagingEnvelope(contentID, envelope, cb),
    (cb) => storage.indexStagingEnvelope(contentID, envelope, cb)
  ], callback);
};
