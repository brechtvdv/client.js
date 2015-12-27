var HttpFetcher = require('./http/HttpFetcher'),
    HttpConnectionsStream = require('./http/HttpConnectionsStream'),
    HttpNeighbouringConnectionsStream = require('./http/HttpNeighbouringConnectionsStream'),
    HttpEntryPoint = require('./http/HttpEntryPoint'),
    MergeStream = require('csa').MergeStream,
    util = require('util'),
    EventEmitter = require('events');

var Fetcher = function (config) {
  EventEmitter.call(this);
  this._config = config;
  this._entrypoints = [];
  for (var k in config.entrypoints) {
    this._entrypoints.push(config.entrypoints[k]);
  }
  this._http = new HttpFetcher(20); // 20 concurrent requests max.
  var self = this;
  this._http.on('request', function (url) {
    self.emit('request', url);
  });
  this._http.on('response', function (url) {
    self.emit('response', url);
  });
  this._connectionsStreams = []; // Holds array of [ stream name, stream ]
  this._mergeStream = null;
}

util.inherits(Fetcher, EventEmitter);

Fetcher.prototype.close = function () {
  for (var k in this._connectionsStreams) {
    this._connectionsStreams[k][1].close();
  }
};

/*
 * Fetches a connectionstream by selecting fragments by time
 */
Fetcher.prototype.buildConnectionsStreamRegular = function (query, cb) {
  //Get the connections from the Web
  var self = this;
  for (var k in this._entrypoints) {
    var entry = new HttpEntryPoint(this._entrypoints[k], this._http);
    entry.fetchFirstUrl(query.departureTime).then(function (url) {
      var connectionsStream = new HttpConnectionsStream(url, self._http);
      self._connectionsStreams.push([url, connectionsStream]); // Uses url as stream name
      if (self._connectionsStreams.length === 1) {
        self._connectionsStreams[0][1].on('data', function (connection) {
          self.emit('data', connection);
        });
        self._connectionsStreams[0][1].on('error', function () {
          self.emit('error');
        });
        cb(self._connectionsStreams[0][1]); // Only one stream
      } else if (self._connectionsStreams.length === self._entrypoints) {
        self._mergeStream = new MergeStream(self._connectionsStreams, query.departureTime);
        self._mergeStream.on('data', function (connection) {
          self.emit('data', connection);
        });
        self._mergeStream.on('error', function () {
          self.emit('error');
        });
        cb(self._mergeStream);
      }
    }, function (error) {
      console.error(error);
    });
  }
};

/*
 * Fetches a connectionstream by selecting fragments by time and stop
 */
Fetcher.prototype.buildConnectionsStreamHeuristic = function (query, cb) {
  var self = this;
  //Get the connections from the Web
  for (var k in this._entrypoints) {
    var connectionsStream = new HttpNeighbouringConnectionsStream(this._entrypoints[k], query, this._http);
    this._connectionsStreams.push([this._entrypoints[k], connectionsStream]);
    connectionsStream.on('data', function (connection) {
      self.emit('data', connection);
    });
    connectionsStream.on('departureStop', function (stop) {
      self.emit('departureStop', stop);
    });
    cb(connectionsStream);
  }
};

/*
 * Fetches a connectionstream by selecting fragments by time
 */
Fetcher.prototype.buildConnectionsStreamSpeedUp = function (query, cb) {
  //Get the connections from the Web
  var self = this;
  for (var k in this._entrypoints) {
    var entry = new HttpEntryPoint(this._entrypoints[k], this._http);
    entry.fetchFirstUrl(query.departureTime, query.departureStop).then(function (url) {
      var connectionsStream = new HttpConnectionsStream(url, self._http);
      self._connectionsStreams.push([url, connectionsStream]); // Uses url as stream name
      if (self._connectionsStreams.length === 1) {
        self._connectionsStreams[0][1].on('data', function (connection) {
          self.emit('data', connection);
        });
        self._connectionsStreams[0][1].on('error', function () {
          self.emit('error');
        });
        cb(self._connectionsStreams[0][1]); // Only one stream
      } else if (self._connectionsStreams.length === self._entrypoints) {
        self._mergeStream = new MergeStream(self._connectionsStreams, query.departureTime);
        self._mergeStream.on('data', function (connection) {
          self.emit('data', connection);
        });
        self._mergeStream.on('error', function () {
          self.emit('error');
        });
        cb(self._mergeStream);
      }
    }, function (error) {
      console.error(error);
    });
  }
};

module.exports = Fetcher;
