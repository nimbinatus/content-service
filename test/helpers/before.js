'use strict';
/*
 * Suite-global initialization that should occur before any other files are required. It's probably
 * a code smell that I have to do this.
 */

const _ = require('lodash');
const config = require('../../src/config');
const logging = require('../../src/logging');

function reconfigure (overrides) {
  if (overrides === undefined) {
    overrides = {};
  }

  if (process.env.INTEGRATION) {
    console.log('Integration test mode active.');

    config.configure(_.merge(process.env, overrides));

    console.log('NOTE: This will leave files uploaded in Cloud Files containers.');
    console.log('Be sure to clear these containers after:');
    console.log('[' + config.contentContainer() + '] and [' + config.assetContainer() + ']');
  } else {
    config.configure(_.merge({
      STORAGE: 'memory',
      RACKSPACE_USERNAME: 'me',
      RACKSPACE_APIKEY: '12345',
      RACKSPACE_REGION: 'space',
      ADMIN_APIKEY: process.env.ADMIN_APIKEY || '12345',
      CONTENT_CONTAINER: 'the-content-container',
      ASSET_CONTAINER: 'the-asset-container',
      MEMORY_ASSET_PREFIX: '/__asset_prefix__/',
      MONGODB_URL: 'mongodb-url',
      CONTENT_LOG_LEVEL: process.env.CONTENT_LOG_LEVEL || 'fatal'
    }, overrides));
  }
}

reconfigure({});
logging.getLogger();

exports.reconfigure = () => reconfigure({});

exports.configureWith = function (overrides) {
  return () => reconfigure(overrides);
};
