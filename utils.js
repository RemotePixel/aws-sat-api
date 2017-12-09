'use strict';

const moment = require('moment');
const AWS = require('aws-sdk');


const zeroPad = (n, c) => {
  let s = String(n);
  if (s.length < c) s = zeroPad('0' + n, c);
  return s;
};


const generate_year_range = (start, end) => {
  const years = [];
  for(var year = start; year <= end; year++){
    years.push(year);
  }
  return years;
};


const aws_list_directory = (bucket, prefix, s3) => {

  const params = {
    Bucket: bucket,
    Delimiter: '/',
    Prefix: prefix};

  return s3.listObjectsV2(params).promise()
    .then(data => {
      return data.CommonPrefixes.map(e => {
        return e.Prefix;
      });
    })
    .catch(() => {
      return [];
    });
};


const get_l8_info = (bucket, key, s3) => {
  const params = {
    Bucket: bucket,
    Key: key};

  return s3.getObject(params).promise()
    .then(data => {
      data = JSON.parse(data.Body.toString());
      let metadata = data.L1_METADATA_FILE;
      let info = {
        date: metadata.PRODUCT_METADATA.DATE_ACQUIRED,
        row: zeroPad(metadata.PRODUCT_METADATA.WRS_ROW, 3),
        path: zeroPad(metadata.PRODUCT_METADATA.WRS_PATH, 3),
        cloud_coverage: metadata.IMAGE_ATTRIBUTES.CLOUD_COVER};

      if ('LANDSAT_PRODUCT_ID' in  metadata.METADATA_FILE_INFO) {
        info.scene_id = metadata.METADATA_FILE_INFO.LANDSAT_PRODUCT_ID;
        info.browseURL = `https://landsat-pds.s3.amazonaws.com/c1/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_large.jpg`;
        info.thumbURL = `https://landsat-pds.s3.amazonaws.com/c1/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_small.jpg`;
      } else {
        info.scene_id = metadata.METADATA_FILE_INFO.LANDSAT_SCENE_ID;
        info.browseURL = `https://landsat-pds.s3.amazonaws.com/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_large.jpg`;
        info.thumbURL = `https://landsat-pds.s3.amazonaws.com/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_small.jpg`;
      }
      return info;
    })
    .catch(() => {
      return {};
    });
};


const get_s2_info = (bucket, key, s3) => {
  const params = {
    Bucket: bucket,
    Key: key};

  return s3.getObject(params).promise()
    .then(data => {
      data = JSON.parse(data.Body.toString());
      return {
        date: moment(data.timestamp).format('YYYY-MM-DD'),
        path: data.path,
        utm_zone: data.utmZone,
        grid_square: data.gridSquare,
        latitude_band: data.latitudeBand,
        cloud: data.cloudyPixelPercentage,
        sat: data.productName.slice(0,3),
        browseURL: `https://sentinel-s2-l1c.s3.amazonaws.com/${data.path}/preview.jpg`
      };
    })
    .catch(() => {
      return {};
    });
};


const get_landsat = (path, row) => {
  const s3 = new AWS.S3({region: 'us-west-2'});
  const landsat_bucket = 'landsat-pds';

  const level = ['L8', 'c1/L8'];

  // get list sceneid
  return Promise.all(level.map(e => {
    let prefix = `${e}/${path}/${row}/`;
    return utils.aws_list_directory(landsat_bucket, prefix, s3);
  }))
    .then(dirs => {
      dirs = [].concat.apply([], dirs);
      return Promise.all(dirs.map(e => {
        let landsat_id = e.split('/').slice(-2,-1)[0];
        let json_path = `${e}${landsat_id}_MTL.json`;
        return utils.get_l8_info(landsat_bucket, json_path, s3);
      }));
    });
};


const get_sentinel = (utm, grid, lat) => {
  const s3 = new AWS.S3({region: 'eu-central-1'});
  const sentinel_bucket = 'sentinel-s2-l1c';
  const img_year = utils.generate_year_range(2015, moment().year());

  // get list of month
  return Promise.all(img_year.map(e => {
    let prefix = `tiles/${utm}/${lat}/${grid}/${e}/`;
    return utils.aws_list_directory(sentinel_bucket, prefix, s3);
  }))
    .then(dirs => {
      // get list of day
      dirs = [].concat.apply([], dirs);
      return Promise.all(dirs.map(e => {
        return utils.aws_list_directory(sentinel_bucket, e, s3);
      }));
    })
    .then(dirs => {
      // get list of version
      dirs = [].concat.apply([], dirs);
      return Promise.all(dirs.map(e => {
        return utils.aws_list_directory(sentinel_bucket, e, s3);
      }));
    })
    .then(data => {
      //create list of image
      data = [].concat.apply([], data);
      return Promise.all(data.map(e => {
        let json_path = `${e}tileInfo.json`;
        return utils.get_s2_info(sentinel_bucket, json_path, s3);
      }));
    });
};


const utils = {
  generate_year_range: generate_year_range,
  aws_list_directory:  aws_list_directory,
  get_s2_info:         get_s2_info,
  get_l8_info:         get_l8_info,
  get_landsat:         get_landsat,
  get_sentinel:        get_sentinel
};
module.exports = utils;
