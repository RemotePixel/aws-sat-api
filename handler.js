'use strict';

const logger = require('fastlog')('sat-api');
const utils = require('./utils.js');

/**
 * landsat handler function.
 *
 * @param {object} event - input
 * @param {object} context -
 * @param {function} callback -
 */

module.exports.landsat = (event, context, callback) => {
  logger.info('Received event: ' + JSON.stringify(event));

  if (event.row === '') return callback(new Error('ROW param missing!'));
  if (event.path === '') return callback(new Error('PATH param missing!'));

  utils.get_landsat(event.path, event.row)
    .then(data => {
      return callback(null, {
        request: { path: event.path, row: event.row },
        meta: { found: data.length },
        results: data
      });
    })
    .catch(err => {
      logger.error(err);
      return callback(new Error('API Error'));
    });
};

/**
 * sentinel handler function.
 *
 * @param {object} event - input
 * @param {object} context -
 * @param {function} callback -
 */

module.exports.sentinel = (event, context, callback) => {
  logger.info('Received event: ' + JSON.stringify(event));

  if (event.utm === '') return callback(new Error('UTM param missing!'));
  if (event.grid === '') return callback(new Error('GRID param missing!'));
  if (event.lat === '') return callback(new Error('LAT param missing!'));

  utils.get_sentinel(event.utm, event.grid, event.lat)
    .then(data => {
      return callback(null, {
        request: { utm: event.utm, grid: event.grid, lat: event.lat },
        meta: { found: data.length },
        results: data
      });
    })
    .catch(err => {
      logger.error(err);
      return callback(new Error('API Error'));
    });
};
