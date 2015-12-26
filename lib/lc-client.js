var Planner = require('csa').BasicCSA,
    Fetcher = require('./Fetcher');

var OptimizedClient = function (config) {
  // Validate config
  this._config = config;
}

OptimizedClient.prototype.query = function (q, cb) {
  // Create fetcher
  var fetcher = new Fetcher(this._config);

  //1. Validate query
  if (q.departureTime) {
    q.departureTime = new Date(q.departureTime);
  } else {
    throw "Date of departure not set";
  }
  if (!q.departureStop) {
    throw "Location of departure not set";
  }
  var query = q, self = this;

  //2. Use query to configure the data fetchers
  fetcher.buildConnectionsStreamOptimized(q, function (connectionsStream) {
    //3. fire results using CSA.js and return the stream
    var planner = new Planner(q);
    //When a result is found, stop the stream
    planner.on('result', function () {
      fetcher.close();
    });
    planner.on('data', function (connection) {
      connectionsStream.analyse(connection); // Minimum Spanning Tree connection
    });
    planner.on('scan', function () {
      connectionsStream.update();
    })
    connectionsStream.on('departureStop', function (stop) {
      planner.emit('departureStop', stop);
    });
    cb(connectionsStream.pipe(planner), fetcher);
  });
};

if (typeof window !== "undefined") {
  // window.lc already defined for the regular implementation
  if (window.lc !== "undefined") {
    window.lc.OptimizedClient = OptimizedClient
  } else {
    window.lc = {
      OptimizedClient : OptimizedClient
    };
  }
}

module.exports = OptimizedClient;
