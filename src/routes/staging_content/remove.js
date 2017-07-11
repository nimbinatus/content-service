/* jshint node: true, esversion: 6*/

'use strict';

const async = require('async');
const storage = require('../../storage');

exports.handler = function (req, res, next) {
  const contentID = req.params.id;
  const prefix = req.query.prefix;
  req.logger.debug('Staging content deletion request received.', { contentID, prefix });

  const handleError = (err, message) => {
    err.statusCode = err.statusCode || err.status || 500;

    req.logger.reportError(message, err, { payload: { contentID, prefix } });
    return next(err);
  };

  const completeRemoval = (contentIDs) => {
    removeStagingEnvelopes(contentIDs, (err) => {
      if (err) return handleError(err, 'Unable to delete staging content.');

      res.send(204);

      req.logger.reportSuccess('Staging content deletion successful.', {
        count: contentIDs.length,
        contentIDs
      });
      next();
    });
  };

  if (!prefix) {
    completeRemoval([contentID]);
  } else {
    const contentIDs = [];
    storage.listStagingEnvelopes({ prefix: contentID }, (envelope) => {
      contentIDs.push(envelope.contentID);
    }, (err) => {
      if (err) return handleError(err, 'Unable to list envelopes.');

      completeRemoval(contentIDs);
    });
  }
};

const removeStagingEnvelopes = exports.removeStagingEnvelopes = function (contentIDs, callback) {
  if (contentIDs.length === 0) {
    return process.nextTick(callback);
  }

  var kvDelete = (cb) => {
    if (contentIDs.length === 1) {
      storage.deleteStagingEnvelope(contentIDs[0], cb);
    } else {
      storage.bulkDeleteStagingEnvelopes(contentIDs, cb);
    }
  };

  var ftsDelete = (cb) => {
    if (contentIDs.length === 1) {
      storage.unindexStagingEnvelope(contentIDs[0], cb);
    } else {
      storage.bulkUnindexStagingEnvelopes(contentIDs, cb);
    }
  };

  async.parallel([
    kvDelete,
    ftsDelete
  ], callback);
};
