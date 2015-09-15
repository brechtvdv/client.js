var Readable = require('stream').Readable,
    jsonld = require('jsonld'),
    util = require('util')

//This class fetches 1 stream of Connections from a Connections Stream Server
//A jsonld-stream is generated
var HttpConnectionsStream = function (starturl, http) {
  Readable.call(this, {objectMode: true});
  this._url = starturl;
  this._http = http; //http fetcher with a limited amount of concurrent requests
  this._connections = [];
  this._pageContext = {};
  this["@context"] = {
    "lc" : "http://semweb.mmlab.be/ns/linkedconnections#",
    "Connection" : "http://semweb.mmlab.be/ns/linkedconnections#Connection",
    "gtfs" : "http://vocab.gtfs.org/terms#",
    "arrivalTime" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalTime",
    "arrivalStop" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalStop",
    "departureTime" : "http://semweb.mmlab.be/ns/linkedconnections#departureTime",
    "departureStop" : "http://semweb.mmlab.be/ns/linkedconnections#departureStop",
    "hydra" : "http://www.w3.org/ns/hydra/core#"
  };
}

util.inherits(HttpConnectionsStream, Readable);

HttpConnectionsStream.prototype._fetchNextPage = function () {
  var self = this;
  console.error('Getting page: ' + this._url);
  return this._http.get(self._url).then(function (result) {
    //check content-type
    var json = JSON.parse(result.body);
    //look for the next page
    return jsonld.promises.compact(json, {"@context": self["@context"]}).then(function (compacted) {
      if (typeof compacted["hydra:nextPage"] !== "undefined") {
        self._url = compacted["hydra:nextPage"];
      } else {
        //stream should stop here
        console.error('no next page link at ' + self._url);
        self.push(null);
      }
      return compacted["@graph"];
    });
  }, function (error) {
    //we have received an error, let's close the stream and output the error
    console.error(error);
    self.push(null);
  });
};

HttpConnectionsStream.prototype._pushNewConnection = function (connection) {
  if (connection["departureTime"]) {
    connection["departureTime"] = new Date(connection["departureTime"]);
  }
  if (connection["arrivalTime"]) {
    connection["arrivalTime"] = new Date(connection["arrivalTime"]);
  }
  this.push(connection);
}

HttpConnectionsStream.prototype._read = function () {
  if (this._connections.length === 0) {
    //can we do something smarter with prefetching data?
    var self = this;
    this._fetchNextPage().then(function (connections) {
      if (connections.length === 0) {
        console.error('end of the stream: empty page encountered');
        self.push(null);
      } else {
        self._connections = connections;
        self._pushNewConnection(self._connections.shift());
      }
    });
  } else {
    this._pushNewConnection(this._connections.shift());
  }
};

module.exports = HttpConnectionsStream;