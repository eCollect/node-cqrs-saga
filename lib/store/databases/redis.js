const { result } = require('lodash');

var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  debug = require('debug')('saga:redis'),
  uuid = require('uuid').v4,
  ConcurrencyError = require('../../errors/concurrencyError'),
  jsondate = require('jsondate'),
  async = require('async'),
  redis = Store.use('redis');

const { resolvify, rejectify } = require('../../helpers').async(debug);

function Redis(options) {
  Store.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 6379,
    prefix: 'saga',
    retry_strategy: function (/* retries, cause */) {
      return false;
    }//,
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  if (options.url) {
    var url = require('url').parse(options.url);
    if (url.protocol === 'redis:') {
      if (url.auth) {
        var userparts = url.auth.split(':');
        options.user = userparts[0];
        if (userparts.length === 2) {
          options.password = userparts[1];
        }
      }
      options.host = url.hostname;
      options.port = url.port;
      if (url.pathname) {
        options.db = url.pathname.replace('/', '', 1);
      }
    }
  }

  this.options = options;
}

util.inherits(Redis, Store);

_.extend(Redis.prototype, {

  connect: function (callback) {
    var self = this;

    var options = this.options;

    this.client = redis.createClient({
			socket: {
				port: options.port || options.socket,
				host: options.host,
        reconnectStrategy: options.retry_strategy,
			},
			database: options.db,
      username: options.username,
      password: options.password,
			// legacyMode: true,
		});

    this.prefix = options.prefix;

    var calledBack = false;

    this.client.on('end', function () {
      self.disconnect();
      self.stopHeartbeat();
    });

    this.client.on('error', function (err) {
      console.log(err);

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });

    this._connect().then(() => {
      self.emit('connect');

      if (self.options.heartbeat) {
        self.startHeartbeat();
      }

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });
  },

  _connect: async function() {
		if (!this.client.isOpen)
			await this.client.connect();
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
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (redis)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.client.ping().then(() => {
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

    if (this.client && this.client.isOpen)
        this.client.quit();

    this.emit('disconnect');
    if (callback) callback(null, this);
  },

  _getNewIdAsync: async function() {
    await this._connect();
    const id = await this.client.incr('nextItemId:' + this.prefix);
    return id.toString();
  },

  getNewId: function(callback) {
    this._getNewIdAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _saveAsync: async function (saga, sagaKey, cmdMap) {
    await this._connect();

    const s = await this._getAsync(saga.id);
    if ((s && saga._hash && saga._hash !== s._hash) || (!s && saga._hash) || (s && s._hash && !saga._hash))
      throw new ConcurrencyError();

    try {
        await this.client.watch(sagaKey)
        saga._hash = uuid().toString();
        var args = [[sagaKey, JSON.stringify(saga)]].concat(cmdMap);
        const multi = this.client.multi();
        multi.mSet(_.fromPairs(args));
        await multi.exec();
    } catch (err) {
        debug(err);
        throw new ConcurrencyError();
    }
  },

  save: function (saga, cmds, callback) {
    if (!saga || !_.isObject(saga) || !_.isString(saga.id) || !_.isDate(saga._commitStamp)) {
      var err = new Error('Please pass a valid saga!');
      debug(err);
      return callback(err);
    }

    if (!cmds || !_.isArray(cmds)) {
      var err = new Error('Please pass a valid saga!');
      debug(err);
      return callback(err);
    }

    if (cmds.length > 0) {
      for (var c in cmds) {
        var cmd = cmds[c];
        if (!cmd.id || !_.isString(cmd.id) || !cmd.payload) {
          var err = new Error('Please pass a valid commands array!');
          debug(err);
          return callback(err);
        }
      }
    }

    var self = this;

    var sagaKey;
    if (saga._timeoutAt) {
      sagaKey = this.options.prefix + '_saga' + ':' +  saga._commitStamp.getTime() + ':' + saga._timeoutAt.getTime() + ':' + saga.id;
    } else {
      sagaKey = this.options.prefix + '_saga' + ':' +  saga._commitStamp.getTime() + ':Infinity:' + saga.id;
    }

    var cmdMap = [];

    _.each(cmds, function (cmd) {
      cmd.payload._sagaId = saga.id;
      cmd.payload._commandId = cmd.id;
      cmd.payload._commitStamp = saga._commitStamp;
      cmdMap.push([
        self.options.prefix + '_command' + ':' + cmd.payload._sagaId+ ':' + cmd.payload._commandId,
        JSON.stringify(cmd.payload)
      ]);
    });

    this._saveAsync(saga, sagaKey, cmdMap)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _scan: async function (pattern, handleKeys) {
    await this._connect();

    for await (const key of this.client.scanIterator({
        MATCH: pattern,
    })) {
        await handleKeys(key);
    }
  },

  _getAsync: async function(id) {
    if (!id || !_.isString(id))
      throw new Error('Please pass a valid id!');

    var allKeys = [];

    await this._scan(this.options.prefix + '_saga:*:*:' + id,
      async function (key) {
        allKeys.push(key);
      });
      
    if (allKeys.length === 0)
      return null;

    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    const saga = await this.client.get(allKeys[0]);
    if (!saga)
      return null;
    
    return jsondate.parse(saga.toString());
  },

  get: function (id, callback) {
    this._getAsync(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _removeAsync: async function (id) {
    if (!id || !_.isString(id))
      throw new Error('Please pass a valid id!');

    return Promise.all([
      this._scan(this.options.prefix + '_saga:*:*:' + id, async (key) => {
        await this.client.del(key);
      }),
      this._scan(this.options.prefix + '_command:' + id + ':*', async (key) => {
        await this.client.del(key);
      })
    ]);
  },

  remove: function (id, callback) {
    this._removeAsync(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getTimeoutedSagasAsync: async function (options) {
    var res = [];
    var allKeys = [];

    await this._scan(this.options.prefix + '_saga:*:*:*',
      async function (key) {
        allKeys.push(key);
      });

    if (allKeys.length === 0)
        return res;
      
    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    if (options.limit === -1) {
      allKeys = allKeys.slice(options.skip);
    } else {
      allKeys = allKeys.slice(options.skip, options.skip + options.limit);
    }

    if (allKeys.length === 0)
      return [];

    for (const key of allKeys) {
      var parts = key.split(':');
      var timeoutAtMs = parts[2];
      var sagaId = parts[3];

      if (timeoutAtMs === 'Infinity')
        timeoutAtMs = Infinity;

      if (_.isString(timeoutAtMs))
        timeoutAtMs = parseInt(timeoutAtMs, 10);

      if (timeoutAtMs > (new Date()).getTime())
        continue;

      const saga = await this._getAsync(sagaId);
      if (saga)
        res.push(saga);
    }

    return res;
  },

  getTimeoutedSagas: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};
    options.limit = options.limit || -1;
    options.skip = options.skip || 0;

    this._getTimeoutedSagasAsync(options)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getOlderSagasAsync: async function (date) {
    if (!date || !_.isDate(date))
      throw new Error('Please pass a valid date object!');

    var res = [];
    var allKeys = [];

    await this._scan(this.options.prefix + '_saga:*:*:*',
      async function (key) {
        allKeys.push(key);
      });
      
    if (allKeys.length === 0)
      return res;
      
    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    for (const key of allKeys) {
        var parts = key.split(':');
        var commitStampMs = parts[1];
        var sagaId = parts[3];

        if (commitStampMs === 'Infinity')
          commitStampMs = Infinity;

        if (_.isString(commitStampMs))
          commitStampMs = parseInt(commitStampMs, 10);

        if (commitStampMs > date.getTime())
          continue;

        const saga = await this._getAsync(sagaId);
        if (saga)
          res.push(saga);
    }

    return res;
  },

  getOlderSagas: function (date, callback) {
    this._getOlderSagasAsync(date)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getUndispatchedCommandsAsync: async function (options) {
    var res = [];
    var allKeys = [];

    await this._scan(this.options.prefix + '_command:*:*',
      async function (key) {
        allKeys.push(key);
      });

    allKeys = _.sortBy(allKeys, function (s) {
      return s;
    });

    if (options.limit === -1) {
      allKeys = allKeys.slice(options.skip);
    } else {
      allKeys = allKeys.slice(options.skip, options.skip + options.limit);
    }

    if (allKeys.length === 0)
      return [];

    for (const key of allKeys) {
      const data = await this.client.get(key);
      if (!data)
          continue;
      const item = jsondate.parse(data.toString());
      res.push({ sagaId: item._sagaId, commandId: item._commandId, command: item, commitStamp: item._commitStamp });
    }

    return res;
  },

  getUndispatchedCommands: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};
    options.limit = options.limit || -1;
    options.skip = options.skip || 0;

    this._getUndispatchedCommandsAsync(options)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _setCommandToDispatchedAsync: async function (cmdId, sagaId) {
    if (!cmdId || !_.isString(cmdId))
      throw new Error('Please pass a valid command id!');

    if (!sagaId || !_.isString(sagaId))
      throw new Error('Please pass a valid saga id!');

    await this._connect();
    await this.client.del(this.options.prefix + '_command:' + sagaId + ':' + cmdId);
  },

  setCommandToDispatched: function (cmdId, sagaId, callback) {
    this._setCommandToDispatchedAsync(cmdId, sagaId)
      .then(() => {
        if (callback) callback();
      })
      .catch(rejectify(callback));
  },

  _clearAsync: async function () {
    const promises = [
      this.client.del('nextItemId:' + this.options.prefix)
    ];

    const keys = await this.client.keys(this.options.prefix + '_saga:*');
    for (const key of keys)
      promises.push(this.client.del(key))

    const cmdKeys = await this.client.keys(this.options.prefix + '_command:*');
    for (const key of cmdKeys)
      promises.push(this.client.del(key));

    await this._connect();
    return Promise.all(promises);
  },

  clear: function (callback) {
    this._clearAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }
});

module.exports = Redis;
