var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  debug = require('debug')('saga:mongodb'),
  ConcurrencyError = require('../../errors/concurrencyError'),
  mongo = Store.use('mongodb'),
  mongoVersion = Store.use('mongodb/package.json').version,
  ObjectId = mongo.ObjectId;

const { resolvify, rejectify } = require('../../helpers').async(debug);

function Mongo(options) {
  Store.call(this, options);

  var defaults = {
    host: '127.0.0.1',
    port: 27017,
    dbName: 'domain',
    collectionName: 'saga'//,
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
    await this.store.createIndex({ '_commands.id': 1}).catch(() => {});
    await this.store.createIndex({ '_timeoutAt': 1}).catch(() => {});
    await this.store.createIndex({ '_commitStamp': 1}).catch(() => {});

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

  _saveAsync: async function (saga, cmds) {
    if (!saga || !_.isObject(saga) || !_.isString(saga.id) || !_.isDate(saga._commitStamp))
      throw new Error('Please pass a valid saga!');

    if (!cmds || !_.isArray(cmds))
      throw new Error('Please pass a valid saga!');

    if (cmds.length > 0) {
      for (var c in cmds) {
        var cmd = cmds[c];
        if (!cmd.id || !_.isString(cmd.id) || !cmd.payload)
          throw Error('Please pass a valid commands array!');
      }
    }

    saga._id = saga.id;
    saga._commands = cmds;

    if (!saga._hash) {
      saga._hash = new ObjectId().toHexString();
      await this.store.insertOne(saga, { writeConcern: { w: 1 } });
      return;
    } 

    var currentHash = saga._hash;
    saga._hash = new ObjectId().toHexString();
    const res = await this.store.updateOne({ _id: saga._id, _hash: currentHash }, { $set: saga }, { writeConcern: { w: 1 } });
    if (res.modifiedCount === 0)
        throw new ConcurrencyError();
  },

  save: function (saga, cmds, callback) {
    this._saveAsync(saga, cmds)
      .then(resolvify(callback))
      .catch((err) => {
          if (err.message && err.message.indexOf('duplicate key') >= 0)
            return callback(new ConcurrencyError());
          return callback(err);
      });
  },

  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    this.store.findOne({ _id: id }).then((saga) => {
      if (!saga)
        return callback(null, null);

      if (saga._commands)
        delete saga._commands;

      callback(null, saga);
    }).catch(rejectify(callback));
  },

  remove: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      return callback(err);
    }

    this.store.deleteOne({ _id: id }, { writeConcern: { w: 1 } })
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  getTimeoutedSagas: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};

    this.store.find({
      _timeoutAt: { '$lte': new Date() }
    }, options).sort({ _timeoutAt: 1 }).toArray().then((sagas) => {
      sagas.forEach(function (s) {
        if (s._commands) {
          delete s._commands;
        }
      });

      callback(null, sagas);
    }).catch(rejectify(callback));
  },

  getOlderSagas: function (date, callback) {
    if (!date || !_.isDate(date)) {
      var err = new Error('Please pass a valid date object!');
      debug(err);
      return callback(err);
    }

    this.store.find({
      _commitStamp: { '$lte': date }
    }).toArray().then((sagas) => {

      sagas.forEach(function (s) {
        if (s._commands) {
          delete s._commands;
        }
      });

      callback(null, sagas);
    }).catch(rejectify(callback));
  },

  getUndispatchedCommands: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};

    var res = [];

    this.store.find({
      '_commands.0': {$exists: true}
    }, options).sort({ _commitStamp: 1 }).toArray().then((sagas) => {
      sagas.forEach((s) => {
        if (s._commands && s._commands.length > 0) {
          s._commands.forEach(function (c) {
            res.push({ sagaId: s._id, commandId: c.id, command: c.payload, commitStamp: s._commitStamp });
          });
        }
      });

      callback(null, res);
    }).catch(rejectify(callback));
  },

  setCommandToDispatched: function (cmdId, sagaId, callback) {
    if (!cmdId || !_.isString(cmdId)) {
      var err = new Error('Please pass a valid command id!');
      debug(err);
      return callback(err);
    }

    if (!sagaId || !_.isString(sagaId)) {
      var err = new Error('Please pass a valid saga id!');
      debug(err);
      return callback(err);
    }

    this.store.updateOne({ _id: sagaId, '_commands.id': cmdId }, { $pull: { '_commands': { id: cmdId } } }, { writeConcern: { w: 1 } })
      .then(() => {
        return callback();
      })
      .catch(rejectify(callback));
  },

  clear: function (callback) {
    this.store.deleteMany({}, { writeConcern: { w: 1 } })
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }
});

module.exports = Mongo;
