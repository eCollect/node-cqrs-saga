var util = require('util'),
    Store = require('../base'),
    _ = require('lodash'),
    debug = require('debug')('saga:revisionGuardStore:redis'),
    ConcurrencyError = require('../../errors/concurrencyError'),
    jsondate = require('jsondate'),
    redis = Store.use('redis');

const { resolvify, rejectify } = require('../../helpers').async(debug);

function Redis(options) {
  Store.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 6379,
    prefix: 'readmodel_revision',
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

  _getAsync: async function (id) {
    if (!id || !_.isString(id))
      throw new Error('Please pass a valid id!');

    await this._connect();
    const entry = await this.client.get(this.options.prefix + ':' + id);
    if (!entry)
      return null;

    const res = jsondate.parse(entry.toString());
    return res.revision || null;
  },

  get: function (id, callback) {
    this._getAsync(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _setAsync: async function (id, revision, oldRevision) {
    if (!id || !_.isString(id))
      throw new Error('Please pass a valid id!');

    if (!revision || !_.isNumber(revision))
      throw new Error('Please pass a valid revision!');

    var key = this.options.prefix + ':' + id;

    const rev = await this._getAsync(id);
    if (rev && rev !== oldRevision)
      throw new ConcurrencyError();

    try {
        await this.client.watch(key)
        const multi = this.client.multi();
        multi.set(key, JSON.stringify({ revision: revision }));
        await multi.exec();
    } catch (err) {
        debug(err);
        throw new ConcurrencyError();
    }
  },

  set: function (id, revision, oldRevision, callback) {
    this._setAsync(id, revision, oldRevision)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _saveLastEventAsync: async function (evt) {
    var key = this.options.prefix + ':THE_LAST_SEEN_EVENT';
    
    await this._connect();
    await this.client.set(key, JSON.stringify({ event: evt }));
  },

  saveLastEvent: function (evt, callback) {
    this._saveLastEventAsync(evt)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getLastEventAsync: async function () {
    await this._connect();
    const entry = await this.client.get(this.options.prefix + ':THE_LAST_SEEN_EVENT');
    if (!entry)
      return null;

    const res = jsondate.parse(entry.toString());
    return res.event || null;
  },

  getLastEvent: function (callback) {
    this._getLastEventAsync()
      .then(resolvify(callback))  
      .catch(rejectify(callback));
  },

  _clearAsync: async function() {
    const promises = [
        this.client.del('nextItemId:' + this.options.prefix)
    ];

    const keys = await this.client.keys(this.options.prefix + ':*');
    for (const key of keys)
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
