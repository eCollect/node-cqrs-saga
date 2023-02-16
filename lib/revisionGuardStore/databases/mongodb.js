var util = require('util'),
    Store = require('../base'),
    _ = require('lodash'),
    debug = require('debug')('saga:revisionGuardStore:mongodb'),
    ConcurrencyError = require('../../errors/concurrencyError'),
    mongo = Store.use('mongodb'),
    mongoVersion = Store.use('mongodb/package.json').version,
    ObjectId = mongo.ObjectId;

const { resolvify, rejectify } = require('../../helpers').async(debug);

function Mongo(options) {
  Store.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 27017,
    dbName: 'readmodel',
    collectionName: 'revision'//,
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  var defaultOpt = {
    ssl: false
  };

  options.options = options.options || {};

  defaultOpt.useNewUrlParser = true;
  defaultOpt.useUnifiedTopology = true;
  _.defaults(options.options, defaultOpt);

  this.options = options;
}

util.inherits(Mongo, Store);

_.extend(Mongo.prototype, {

  _connectAsync: async function () {
    var options = this.options;

    var connectionUrl;

    if (options.url) {
      connectionUrl = options.url;
    } else {
      var members = options.servers
        ? options.servers
        : [{host: options.host, port: options.port}];

      var memberString = _(members).map(function(m) { return m.host + ':' + m.port; });
      var authString = options.username && options.password
        ? options.username + ':' + options.password + '@'
        : '';
      var optionsString = options.authSource
        ? '?authSource=' + options.authSource
        : '';

      connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
    }

    var client = new mongo.MongoClient(connectionUrl, options.options);
    const cl = await client.connect();
    this.db = cl.db(cl.s.options.dbName);

    if (!this.db.close)
      this.db.close = cl.close.bind(cl);

    cl.on('serverClosed', () => {
      this.emit('disconnect');
      this.stopHeartbeat();
    });

    this.store = this.db.collection(options.collectionName);
    
    this.emit('connect');

    if (this.options.heartbeat)
        this.startHeartbeat();
  },

  connect: function (callback) {
    this._connectAsync().then(() => {
      if (callback) callback(null, this);
    }).catch(rejectify(callback))
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    if (this.heartbeatInterval)
      return;

    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.db.admin().ping()
        .then(() => {
            if (graceTimer) clearTimeout(graceTimer);
        }).catch((err) => {
            if (graceTimer) clearTimeout(graceTimer);

            console.error(err.stack || err);
            self.disconnect();
        });
      
    }, this.options.heartbeat);
  },

  disconnect: function (callback) {
    this.stopHeartbeat();

    if (!this.db) {
      if (callback) callback(null);
      return;
    }

    this.db.close()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  getNewId: function(callback) {
    callback(null, new ObjectId().toHexString());
  },

  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    this.store.findOne({ _id: id }).then((entry) => {
      if (!entry)
        return callback(null, null);

      callback(null, entry.revision || null);
    }).catch(rejectify(callback));
  },

  set: function (id, revision, oldRevision, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }
    if (!revision || !_.isNumber(revision)) {
      var err = new Error('Please pass a valid revision!');
      debug(err);
      return callback(err);
    }

    this.store.replaceOne({ _id: id, revision: oldRevision }, { _id: id, revision: revision }, { 
      upsert: true,
      writeConcern: { w: 1 },
    }).then((res) => {
          if (res.modifiedCount === 0 && res.upsertedCount === 0) {
            err = new ConcurrencyError();
            debug(err);
            if (callback) {
              callback(err);
            }
            return;
          }

        if (callback) { callback(); }
    }).catch((err) => {
        if (err && err.message && err.message.match(/duplicate key/i)) {
          debug(err);
          err = new ConcurrencyError();
          debug(err);
          if (callback) {
            callback(err);
          }
          return;
        }
        if (callback) { callback(err); }
    });
  },

  saveLastEvent: function (evt, callback) {
    this.store.replaceOne({ _id: 'THE_LAST_SEEN_EVENT' }, { _id: 'THE_LAST_SEEN_EVENT', event: evt }, {
      upsert: true,
      writeConcern: { w: 1 }
    }).then(resolvify(callback))
      .catch(rejectify(callback));
  },

  getLastEvent: function (callback) {
    this.store.findOne({ _id: 'THE_LAST_SEEN_EVENT' }).then((entry) => {
      if (!entry)
        return callback(null, null);

      callback(null, entry.event || null);
    }).catch(rejectify(callback));
  },

  clear: function (callback) {
    this.store.deleteMany({}, { writeConcern: { w: 1 } })
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }

});

module.exports = Mongo;
