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


const parseSceneid_c1 = (sceneid) => {
  const sceneid_info = sceneid.split('_');
  return {
    scene_id: sceneid,
    satellite: sceneid_info[0].slice(0,1) + sceneid_info[0].slice(3),
    sensor: sceneid_info[0].slice(1,2),
    correction_level: sceneid_info[1],
    path: sceneid_info[2].slice(0,3),
    row: sceneid_info[2].slice(3),
    acquisition_date: sceneid_info[3],
    ingestion_date: sceneid_info[4],
    collection: sceneid_info[5],
    category: sceneid_info[6]
  };
};


const parseSceneid_pre = (sceneid) => {
  return {
    scene_id: sceneid,
    satellite: sceneid.slice(2,3),
    sensor: sceneid.slice(1,2),
    path: sceneid.slice(3,6),
    row: sceneid.slice(6,9),
    acquisitionYear: sceneid.slice(9,13),
    acquisitionJulianDay: sceneid.slice(13,16),
    acquisition_date: moment().utc().year(sceneid.slice(9,13)).dayOfYear(sceneid.slice(13,16)).format('YYYYMMDD'),
    groundStationIdentifier: sceneid.slice(16,19),
    archiveVersion: sceneid.slice(19,21)
  };
};


const parseCBERSid = (sceneid) => {
  return {
    scene_id: sceneid,
    satellite: sceneid.split('_')[0],
    version: sceneid.split('_')[1],
    sensor: sceneid.split('_')[2],
    path: sceneid.split('_')[4],
    row: sceneid.split('_')[5],
    acquisition_date: sceneid.split('_')[3],
    processing_level: sceneid.split('_')[6]
  };
};


const get_l8_info = (bucket, key, s3) => {
  const params = {
    Bucket: bucket,
    Key: key};

  return s3.getObject(params).promise()
    .then(data => {
      data = JSON.parse(data.Body.toString());
      let metadata = data.L1_METADATA_FILE;
      return {
        cloud_coverage: metadata.IMAGE_ATTRIBUTES.CLOUD_COVER,
        cloud_coverage_land: metadata.IMAGE_ATTRIBUTES.CLOUD_COVER_LAND,
        sun_azimuth: metadata.IMAGE_ATTRIBUTES.SUN_AZIMUTH,
        sun_elevation: metadata.IMAGE_ATTRIBUTES.SUN_ELEVATION,
        geometry: {
          type: 'Polygon',
          coordinates: [[
            [metadata.PRODUCT_METADATA.CORNER_UR_LON_PRODUCT, metadata.PRODUCT_METADATA.CORNER_UR_LAT_PRODUCT],
            [metadata.PRODUCT_METADATA.CORNER_UL_LON_PRODUCT, metadata.PRODUCT_METADATA.CORNER_UL_LAT_PRODUCT],
            [metadata.PRODUCT_METADATA.CORNER_LL_LON_PRODUCT, metadata.PRODUCT_METADATA.CORNER_LL_LAT_PRODUCT],
            [metadata.PRODUCT_METADATA.CORNER_LR_LON_PRODUCT, metadata.PRODUCT_METADATA.CORNER_LR_LAT_PRODUCT],
            [metadata.PRODUCT_METADATA.CORNER_UR_LON_PRODUCT, metadata.PRODUCT_METADATA.CORNER_UR_LAT_PRODUCT]
          ]]
        }};
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
        cloud_coverage: data.cloudyPixelPercentage,
        coverage: data.dataCoveragePercentage,
        geometry : data.tileGeometry,
        sat: data.productName.slice(0,3)};
    })
    .catch(() => {
      return {};
    });
};


const get_landsat = (path, row, full=false) => {
  const s3 = new AWS.S3({region: 'us-west-2'});
  const landsat_bucket = 'landsat-pds';

  row = utils.zeroPad(row, 3);
  path = utils.zeroPad(path, 3);

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
        let info, aws_url;

        if (/L[COTEM]08_L\d{1}[A-Z]{2}_\d{6}_\d{8}_\d{8}_\d{2}_(T1|RT)/.exec(landsat_id)) {
          info = utils.parseSceneid_c1(landsat_id);
          info.type = info.category;
          aws_url = 'https://landsat-pds.s3.amazonaws.com/c1';
        } else {
          info = utils.parseSceneid_pre(landsat_id);
          info.type = 'pre';
          aws_url = 'https://landsat-pds.s3.amazonaws.com';
        }
        info.browseURL = `${aws_url}/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_large.jpg`;
        info.thumbURL = `${aws_url}/L8/${info.path}/${info.row}/${info.scene_id}/${info.scene_id}_thumb_small.jpg`;

        if (full) {
          let json_path = `${e}${landsat_id}_MTL.json`;
          return utils.get_l8_info(landsat_bucket, json_path, s3)
            .then(data => {
              return Object.assign({}, info, data);
            });
        } else {
          return info;
        }
      }));
    });
};


const get_cbers = (path, row) => {
  const s3 = new AWS.S3({region: 'us-east-1'});
  const cbers_bucket = 'cbers-meta-pds';

  row = utils.zeroPad(row, 3);
  path = utils.zeroPad(path, 3);

  // get list sceneid
  const prefix = `CBERS4/MUX/${path}/${row}/`;
  return utils.aws_list_directory(cbers_bucket, prefix, s3)
    .then(dirs => {
      dirs = [].concat.apply([], dirs);
      return dirs.map(e => {
        let cbers_id = e.split('/').slice(-2,-1)[0];
        let info = utils.parseCBERSid(cbers_id);
        let preview_id = cbers_id.split('_').slice(0,-1).join('_');
        info.browseURL = `https://${cbers_bucket}.s3.amazonaws.com/CBERS4/MUX/${path}/${row}/${cbers_id}/${preview_id}_small.jpeg`;
        return info;
      });
    });
};

const get_sentinel = (utm, lat, grid, full=false) => {
  const s3 = new AWS.S3({region: 'eu-central-1'});
  const sentinel_bucket = 'sentinel-s2-l1c';
  const img_year = utils.generate_year_range(2015, moment().year());

  utm = utm.replace(/^0/, '');

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
        let s2path = e.replace(/\/$/, '');
        let yeah = s2path.split('/')[4];
        let month = utils.zeroPad(s2path.split('/')[5], 2);
        let day = utils.zeroPad(s2path.split('/')[6], 2);

        let info = {
          path: s2path,
          utm_zone: s2path.split('/')[1],
          latitude_band: s2path.split('/')[2],
          grid_square: s2path.split('/')[3],
          num: s2path.split('/')[7],
          acquisition_date: `${yeah}${month}${day}`,
          browseURL: `https://sentinel-s2-l1c.s3.amazonaws.com/${s2path}/preview.jpg`};

        const utm = utils.zeroPad(info.utm_zone, 2);
        info.scene_id = `S2A_tile_${info.acquisition_date}_${utm}${info.latitude_band}${info.grid_square}_${info.num}`;

        if (full) {
          let json_path = `${e}tileInfo.json`;
          return utils.get_s2_info(sentinel_bucket, json_path, s3)
            .then(data => {
              info = Object.assign({}, info, data);
              info.scene_id = `${info.sat}_tile_${info.acquisition_date}_${utm}${info.latitude_band}${info.grid_square}_${info.num}`;
              return info;
            });
        } else {
          return info;
        }
      }));
    });
};


const utils = {
  generate_year_range: generate_year_range,
  aws_list_directory:  aws_list_directory,
  parseSceneid_pre:    parseSceneid_pre,
  parseSceneid_c1:     parseSceneid_c1,
  parseCBERSid:        parseCBERSid,
  get_sentinel:        get_sentinel,
  get_landsat:         get_landsat,
  get_cbers:           get_cbers,
  get_s2_info:         get_s2_info,
  get_l8_info:         get_l8_info,
  zeroPad:             zeroPad
};
module.exports = utils;
