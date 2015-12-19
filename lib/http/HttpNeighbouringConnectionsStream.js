var Readable = require('stream').Readable,
    HttpEntryPoint = require('./HttpEntryPoint'),
    PriorityQueue = require('priorityqueuejs'),
    jsonld = require('jsonld'),
    util = require('util');

// This class fetches 1 stream of Connections by selecting a different departure stop between every request
// A jsonld-stream is generated
// A query must have coordinates of the start and goal stop
var HttpNeighbouringConnections = function (entrypoint, query, http) {
  Readable.call(this, {objectMode: true});
  this._entrypoint = entrypoint;
  this._goalCoordinates = [query.arrivalStopLongitude, query.arrivalStopLatitude];
  this._bestStops = new PriorityQueue(function(a, b) {
    return a.priority - b.priority;
  }); // a priority queue containing best stops for getting the fastest route
  var departureStop = query.departureStop;
  var departureTime = query.departureTime;
  this._bestStops.enq({'stopId': departureStop, 'minimumDepartureTime': departureTime, 'priority': 3});
  this._http = http; //http fetcher with a limited amount of concurrent requests
  this._connections = [];
  this._entry = new HttpEntryPoint(this._entrypoint, this._http);
  this["@context"] = {
    "lc" : "http://semweb.mmlab.be/ns/linkedconnections#",
    "Connection" : "http://semweb.mmlab.be/ns/linkedconnections#Connection",
    "gtfs" : "http://vocab.gtfs.org/terms#",
    "arrivalTime" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalTime",
    "arrivalStop" : {
      "@id": "http://semweb.mmlab.be/ns/linkedconnections#arrivalStop",
      "@type": "@id"
    },
    "departureTime" : "http://semweb.mmlab.be/ns/linkedconnections#departureTime",
    "departureStop" : {
      "@id": "http://semweb.mmlab.be/ns/linkedconnections#departureStop",
      "@type": "@id"
    },
    "hydra" : "http://www.w3.org/ns/hydra/core#",
    "locationDepartureStop": {
      "@context": "http://schema.org",
      "@id": "http://semweb.mmlab.be/ns/linkedconnections#locationDepartureStop",
      "@type": "GeoCoordinates",
      "latitude": "https://schema.org/GeoCoordinates#latitude",
      "longitude": "https://schema.org/GeoCoordinates#longitude"
    },
    "locationArrivalStop": {
      "@context": "http://schema.org",
      "@id": "http://semweb.mmlab.be/ns/linkedconnections#locationArrivalStop",
      "@type": "GeoCoordinates",
      "latitude": "https://schema.org/GeoCoordinates#latitude",
      "longitude": "https://schema.org/GeoCoordinates#longitude"
    },
    "countDirectStopsArrivalStop": "http://semweb.mmlab.be/ns/linkedconnections#countDirectStopsArrivalStop",
    "countDirectStopsDepartureStop": "http://semweb.mmlab.be/ns/linkedconnections#countDirectStopsDepartureStop"
  };

  // Maximum values of heuristic parameters for normalization
  this._maxSpeed = 0; // speed connection
  this._maxRestDistanceToGoal = 0;
  this._maxCos = 0;
  this._maxImportance = 0;

  this._interval = 10*30; // When retrying a stop, add this interval to new departure time (minutes)
};

util.inherits(HttpNeighbouringConnections, Readable);

HttpNeighbouringConnections.prototype.close = function () {
  this.push(null);
};

HttpNeighbouringConnections.prototype._getNextStop = function () {
  if (this._bestStops.size() > 0) {
    var stop = this._bestStops.deq();
    // Make sure that priority queue is not empty, add last tried stop with lower priority and add interval to departureTime
    this._bestStops.enq({'stopId': stop.stopId, 'minimumDepartureTime': new Date(stop.minimumDepartureTime.getTime() + this._interval*60000), 'priority': stop.priority/2});
    this.emit("departureStop", stop);
    return stop;
  } else {
    this.push(null);
  }
};

/*
 * Calculates the importance of the arrival stop if not already seen
 */
HttpNeighbouringConnections.prototype.analyse = function (connection) {
  var coordsDepartureStop = [connection['locationDepartureStop'].longitude, connection['locationDepartureStop'].latitude];
  var coordsArrivalStop = [connection['locationArrivalStop'].longitude, connection['locationArrivalStop'].latitude];

  // Use Haversine function to calculate distance between the stop and the goal stop
  var restDistanceToGoal = this._haversineDistance(coordsArrivalStop, this._goalCoordinates);
  var distance = this._haversineDistance(coordsDepartureStop, coordsArrivalStop); // kilometers
  var time = (new Date(connection['arrivalTime']) - new Date(connection['departureTime']))/1000; // time difference in hours
  var speed;
  if (time === 0) {
    speed = 0;
  } else {
    speed = distance / time;
  }
  var cos = this._cosineSimilarity(coordsArrivalStop, this._goalCoordinates);
  var countDirectStopsArrivalStop = connection['countDirectStopsArrivalStop'];
  if (countDirectStopsArrivalStop > 150) countDirectStopsArrivalStop = 175; // Brussel has too much advantage over Ghent and Antwerp

  // calculate priority
  var priority = this._calculatePriority(speed, restDistanceToGoal, cos, countDirectStopsArrivalStop);
  if (priority == Infinity) priority = 3;

  // Add to best stops
  this._bestStops.enq({'stopId': connection['arrivalStop'], 'minimumDepartureTime': new Date(connection['arrivalTime']), 'priority': priority});
};

HttpNeighbouringConnections.prototype._calculateMaxValues = function (connections) {
  var N = connections.length > 1000 ? 1000 : connections.length;

  for (var i=0; i<N; i++) {
    var connection = connections[i];

    var coordsDepartureStop = [connection['locationDepartureStop'].longitude, connection['locationDepartureStop'].latitude];
    var coordsArrivalStop = [connection['locationArrivalStop'].longitude, connection['locationArrivalStop'].latitude];

    var restDistanceToGoal = this._haversineDistance(coordsArrivalStop, this._goalCoordinates);
    var distance = this._haversineDistance(coordsDepartureStop, coordsArrivalStop); // kilometers
    var time = (new Date(connection['arrivalTime']) - new Date(connection['departureTime']))/1000; // time difference in hours
    var speed = distance / time;
    var cos = this._cosineSimilarity(coordsArrivalStop, this._goalCoordinates);
    var countDirectStopsArrivalStop = connection['countDirectStopsArrivalStop'];
    if (countDirectStopsArrivalStop > 150) countDirectStopsArrivalStop -= 175; // Brussel has too much advantage over Ghent and Antwerp

    if (restDistanceToGoal > this._maxRestDistanceToGoal) this._maxRestDistanceToGoal = restDistanceToGoal;
    if (speed > this._maxSpeed) this._maxSpeed = speed;
    if (cos > this._maxCos) this._maxCos = cos;
    if (countDirectStopsArrivalStop > this._maxImportance) this._maxImportance = countDirectStopsArrivalStop;
  }
};

/*
 * @param speed Speed of connection (km/h)
 * @param cos Cosine simularity of connection vector and direct way vector
 * @param distanceToGoal
 * @param importance amount of direct stops
 */
HttpNeighbouringConnections.prototype._calculatePriority = function (speed, restDistanceToGoal, cos, importance) {
  // Normalize
  var s = speed/this._maxSpeed;
  var r = restDistanceToGoal/this._maxRestDistanceToGoal;
  var c = cos/this._maxCos;
  var i = importance/this._maxImportance;

  var alpha = 1;
  var beta = -1; // a stop that is located far from the goal is not good
  var gamma = 1;
  var kappa = 1;
  return (alpha*s + beta*r + gamma*c + kappa*i);
};

HttpNeighbouringConnections.prototype._fetchNextPage = function (url) {
  var self = this;
  return this._http.get(url).then(function (result) {

    var document = JSON.parse(result.body);
    // Todo
    // return jsonld.promises.frame(document['@graph'], {"@context":self["@context"], "@type" : "Connection"}).then(function (data) {
    return document["@graph"];
    // });
  }, function (error) {
    //we have received an error, let's choose another stop
    console.error("Error: ", error);
    self._read();
  });
};

HttpNeighbouringConnections.prototype._pushNewConnection = function (connection) {
  if (connection["departureTime"]) {
    connection["departureTime"] = new Date(connection["departureTime"]);
  }
  if (connection["arrivalTime"]) {
    connection["arrivalTime"] = new Date(connection["arrivalTime"]);
  }
  this.push(connection);
}

HttpNeighbouringConnections.prototype._read = function () {
  // No connections available
  if (this._connections.length === 0) {
    var self = this;
    // Decision: which stop is best for requesting connections
    var nextStop = this._getNextStop();
    // For every request we need to ask the correct URL
    this._entry.fetchFirstUrl(nextStop.minimumDepartureTime, nextStop.stopId).then(function (url) {
      self._fetchNextPage(url).then(function (connections) {
        if (connections.length === 0) {
          // Choose another stop
          self._read(); // read another stop
        } else {
          self._connections = connections;
          // Loop over first connections to calculate maximum values for heuristic
          self._calculateMaxValues(connections);
          self._pushNewConnection(self._connections.shift());
        }
      });
    });
  } else {
    this._pushNewConnection(this._connections.shift());
  }
};

// coords: [longitude, latitude]
// Returns amount of kilometers between the two coordinates
HttpNeighbouringConnections.prototype._haversineDistance = function (coords1, coords2) {
  function toRad(x) {
    return x * Math.PI / 180;
  }

  var lon1 = coords1[0];
  var lat1 = coords1[1];

  var lon2 = coords2[0];
  var lat2 = coords2[1];

  var R = 6371; // km

  var x1 = lat2 - lat1;
  var dLat = toRad(x1);
  var x2 = lon2 - lon1;
  var dLon = toRad(x2)
  var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  var d = R * c;

  return d;
};

HttpNeighbouringConnections.prototype._getVector = function (lon, lat){
  var vector = {};
  vector.lon = lon;
  vector.lat = lat;
  vector.length = Math.sqrt(lon*lon + lat*lat);
  // vector.angle = Math.acos( lon / vector.length );
  return vector;
}

HttpNeighbouringConnections.prototype._cosineSimilarity = function (coords1, coords2) {
  var v1 = this._getVector(coords1[0], coords1[1]);
  var v2 = this._getVector(coords2[0], coords2[1]);
	return (v1.lon * v2.lon + v1.lat * v2.lat) / (v1.length + v2.length);
}

module.exports = HttpNeighbouringConnections;
