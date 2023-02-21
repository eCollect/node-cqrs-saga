var debug = require('debug')('saga:structureParser'),
  _ = require('lodash'),
  fs = require('fs/promises'),
  path = require('path');

var validFileTypes = ['js'];

function isValidFileType(fileName) {
  var index = fileName.lastIndexOf('.');
  if (index < 0) {
    return false;
  }
  var fileType = fileName.substring(index + 1);
  index = validFileTypes.indexOf(fileType);
  if (index < 0) {
    return false;
  }
  return validFileTypes[index];
}

async function loadPaths (dir) {
  dir = path.resolve(dir);

  var results = [];
  const list = await fs.readdir(dir);
  var pending = list.length;

  if (pending === 0) return results;

  for (const file of list) {
    // console.log('----', file);
    var pathFull = path.join(dir, file);
    try {
      const stat = await fs.stat(pathFull);

      // if directory, go deep...
      if (stat && stat.isDirectory()) {
        const res = await loadPaths(pathFull);
        results = results.concat(res);
        if (!--pending) return results;
        continue;
      }
  
      // if a file we are looking for
      if (isValidFileType(pathFull))
        results.push(pathFull);
  
      // of just an other file, skip...
      if (!--pending) return results;
    } catch (err) {
      debug(err);
    }
  }
}

function pathToJson (root, paths, addWarning) {
  root = path.resolve(root);
  var res = [];

  paths.forEach(function (p) {
    if (p.indexOf(root) >= 0) {
      var part = p.substring(root.length);
      if (part.indexOf(path.sep) === 0) {
        part = part.substring(path.sep.length);
      }

      var splits = part.split(path.sep);
      var withoutFileName = splits;
      withoutFileName.splice(splits.length - 1);
      var fileName = path.basename(part);

      var dottiedBase = '';
      withoutFileName.forEach(function (s, i) {
        if (i + 1 < withoutFileName.length) {
          dottiedBase += s + '.';
        } else {
          dottiedBase += s;
        }
      });

      try {
        var required = require(p);

//        // clean cache, fixes multiple loading of same aggregates, commands, etc...
//        if (require.cache[require.resolve(p)]) {
//          delete require.cache[require.resolve(p)];
//        }

        if (!required || _.isEmpty(required)) {
          return;
        }

        if (typeof required === 'object' && typeof required.default !== 'undefined') {
          required = required.default;
        }

        if (_.isArray(required)) {
          _.each(required, function (req) {
            res.push({
              path: p,
              dottiedBase: dottiedBase,
              fileName: fileName,
              value: req,
              fileType: path.extname(p).substring(1)
            });
          });
        } else {
          res.push({
            path: p,
            dottiedBase: dottiedBase,
            fileName: fileName,
            value: required,
            fileType: path.extname(p).substring(1)
          });
        }
      } catch (err) {
        debug(err);
        if (addWarning) {
          addWarning(err);
        }
      }
    } else {
      debug('path is not a subpath from root');
    }
  });

  return res;
}

function parse (dir, filter, callback) {
  if (!callback) {
    callback = filter;
    filter = function (r) {
      return r;
    };
  }

  dir = path.resolve(dir);
  loadPaths(dir).then((paths) => {
    var warns = [];
    function addWarning (e) {
      warns.push(e);
    }

    var res = filter(pathToJson(dir, paths, addWarning));

    var dottiesParts = [];

    res.forEach(function (r) {
      var parts = r.dottiedBase.split('.');
      parts.forEach(function (p, i) {
        if (!dottiesParts[i]) {
          return dottiesParts[i] = [p];
        }

        if (dottiesParts[i].indexOf(p) < 0) {
          dottiesParts[i].push(p);
        }
      });
    });

    var toRemove = '';

    for (var pi = 0, plen = dottiesParts.length; pi < plen; pi++) {
      if (dottiesParts[pi].length === 1) {
        toRemove += dottiesParts[pi][0];
      } else {
        break;
      }
    }

    if (toRemove.length > 0) {
      res.forEach(function (r) {
        if (r.dottiedBase === toRemove) {
          r.dottiedBase = '';
        } else {
          r.dottiedBase = r.dottiedBase.substring(toRemove.length + 1);
        }
      });
    }

    if (warns.length === 0) {
      warns = null;
    }

    callback(null, res, warns);
  }).catch((err) => {
    debug(err);
    callback(err);
  });
}

module.exports = parse;
