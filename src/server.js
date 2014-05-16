var express = require('express'),
	app = express(),
	mongodb = require('mongodb'),
	uuid = require('node-uuid'),
	format = require('util').format,
	fs = require('fs'),
	navimonapi = require("./modules/api.js");

app.use(express.bodyParser());
app.set('view engine', 'ejs');

var getDataStream = function getDataStream(db, obj, callback, limit, type, sanitize, averagePeriod) {
	if (!limit) limit = 5;
	if (!type) type = "raw";
    var collection = db.collection('datastreams');
    collection.find(obj, {"_id":1, "name":1, "currentValue": 1, "lastUpdate": 1, "description": 1}).toArray(function(err, results) {
		if (results.length > 0) {
			var doc = results[0],
				findObj = {"streamid": doc._id, "type": type};
			if (type == "average") {
				findObj.averagePeriod = averagePeriod || "10m";
			}
			var datapoints = db.collection("datapoints"),
				cursor = datapoints.find(findObj, {'value': true, 'at': true, 'type': true, 'stddev': true, 'datapoints': true, 'averagePeriod': true});
			//doc.requestTime = new Date();

			if (sanitize) {
				delete doc.apikey;
				delete doc.user;
				delete doc._id;
				doc.at = doc.lastUpdate;
				delete doc.lastUpdate;
				doc.value = doc.currentValue;
				delete doc.currentValue;
			}
			//callback(200, doc);
			
			cursor.sort({'at': -1}).limit(limit).toArray(function(err, results) {
				if (!results) {
					results = [];
				}
				for (var i = 0; i < results.length; i++) {
					var point = results[i];
					if (point.datapoints) {
						point.nPoints = point.datapoints;
						delete point.datapoints;
					}
					if (sanitize) {
						delete point._id;
					}
				}
				doc.datapoints = results;
				callback(200, doc);
			});
		} else {
			callback(404, "Not Found");
		}
	});
};

app.use('/static', express.static(__dirname + '/static'));

app.get('/', function(req, res) {
  res.render('index', { title: 'Welcome!' })
});

app.get('/json', function(req, res){
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		if (err) {
			res.send(500, err.message);
		}
		var groups = db.collection('groups');
		var rc = {};
		groups.find({}).toArray(function(err, grpresults) {
			if (grpresults.length > 0) {
				var nextGroup = function() {
					if (grpresults.length == 0) {
						res.send(200, rc);
						db.close();
						return;
					}
					var group = grpresults.pop();
					rc[group.name] = {streams:{}};
				    var collection = db.collection('datastreams');
			        collection.find({}).toArray(function(err, results) {
						var getNext = function getNext(httpCode, doc) {
							if (doc && httpCode == 200) {
								rc[group.name].streams[doc.name] = doc;
								delete doc.name;
							}
							if (results.length == 0) {
								nextGroup();
							}
							var stream = results.pop();
							if (stream) {
								getDataStream(db, {_id: stream._id, 'group': group._id}, getNext, parseInt(req.query.limit) || 50, req.query.type || "average", true, req.query.granularity || "10m");
							}
						};
						getNext();
					});
				}
				nextGroup();
			} else {
				res.send(200, rc);
				db.close();
				return;
			}
		});
	});
});

/* 
app.get("/fix", function(req, res) {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
	    var datapoints = db.collection('datapoints');
		datapoints.find({}).toArray(function(err, results) {
			var cb = function() {
				if (results.length > 0) {
					var point = results.pop()
					if (parseInt("" + point.value) == point.value) {
						point.streamid = new mongodb.ObjectID("533c3b62147d4b0870f5c7b8");
					} else if (parseFloat("" + point.value) == point.value) {
						point.streamid = new mongodb.ObjectID("533b6a328346d18a69983fd1");
					}
					datapoints.update({"_id":  point._id}, point, {}, cb);
				} else {
					datapoints.find({}).toArray(function(err, results) {
						res.send(200,results);
						db.close();
					});
				}
			}
			cb();
		});
	});
});
 */
/* functions to create containers */
app.get('/group/:groupname/create', function(req, res) {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		var collection = db.collection('groups');
        collection.find({"name": req.params.groupname}).toArray(function(err, results) {
			if (results.length > 0) {
				res.send(409, results[0]);
				db.close();
			} else {
				var obj = {
					"name": req.params.groupname,
					"user": "timon",
					apikey: uuid.v4(),
					created: new Date()
				};
			    collection.insert(obj, function(err, docs) {
					res.send(200, docs[0]);
					db.close();
			    });
			}
		});
	});
});

app.get('/group/:groupname/stream/:streamname/create', function(req, res) {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		if (err) {
			res.send(500, err.message);
		}
		var groups = db.collection('groups');
		groups.find({'name': req.params.groupname}).toArray(function(err, groupresults) {
			if (groupresults.length > 0) {
				var group = groupresults[0],
					collection = db.collection('datastreams');
		        collection.find({"name": req.params.streamname, "group": group._id}).toArray(function(err, results) {
					if (results.length > 0) {
						res.send(409, results[0]);
						db.close();
					} else {
						var obj = {
							"name": req.params.streamname,
							"group": group._id,
							apikey: uuid.v4(),
							created: new Date()
						};
					    collection.insert(obj, function(err, docs) {
							res.send(200, docs[0]);
							db.close();
					    });
					}
				});
			} else {
				// no such group
				res.send(400, {error: "no such group"});
				db.close();
			}
		});
	});
});

/* FIXME: test this function, untested so far */
app.post('/stream', function(req, res) {
	if (req.is('application/json')) {
		if (req.body.length > 0) {
			mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
				if (err) {
					res.send(500, err.message);
				}
			    var datastreams = db.collection('datastreams');
				var streams = req.body;
				var next = function() {
					if (streams.length > 0) {
						var stream = streams.pop();
						datastreams.find({"_id": new mongodb.ObjectID(stream._id)}).toArray(function(err, results) {
							if (results.length > 0) {
								var compstream = results[0];
								if (stream.apikey == stream.apikey && stream.datapoints) {
									stream.name = compstream.name;
									var datapoints = db.collection("datapoints");
									var nextpoint = function() {
										if (stream.datapoints.length == 0) {
											next();
											return;
										}
										var point = stream.datapoints.pop();
										if (!point.at) {
											point.at = new Date();
										}
										if (parseInt(point.value) == point.value) {
											point.value = parseInt(point.value);
										} else if (parseFloat(point.value) == point.value) {
											point.value = parseFloat(point.value);
										}
										navimonapi.addDataPoint(stream, point, db, function() {
											nextpoint();
										});
									};
									nextpoint();
								} else {
									res.send(403, "Invalid API Key for feed");
									db.close();
								}
							} else {
								next();
							}
						});
					} else {
						res.send(200, "Accepted");
						db.close();
					}
				};
				next();
			});
		}
	} else {
		res.send(406, 'Not Acceptable')
	}
});

app.get('/group/:groupname/stream/:streamname', function(req, res) {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		if (err) {
			res.send(500, err.message);
		}
		var groups = db.collection('groups');
		var rc = {};
		groups.find({name: req.params.groupname}).toArray(function(err, grpresults) {
			if (grpresults.length > 0) {
				var group = grpresults[0];
				var getStreamCallback = function getStreamCallback(httpCode, result) {
					res.send(httpCode, result);
					db.close();
				}
				getDataStream(db, {'name': req.params.streamname, 'group': group._id}, getStreamCallback, parseInt(req.query.limit) || 5, req.query.type || 'average', true);	
			}
		});
	});
});

app.post('/group/:groupname/streams/:streamname', function(req, res) {
	// handle  POST using only stream's apikey, validate request to make sure that they are going through the correct group
});

app.get('/group/:groupname', function(req, res) {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		if (err) {
			res.send(500, err.message);
		}
		var groups = db.collection('groups');
		var rc = {streams:{}};
		groups.find({name: req.params.groupname}).toArray(function(err, grpresults) {
			if (grpresults.length > 0) {
				var group = grpresults[0],
					datastreams = db.collection('datastreams');
				rc.apikey = group.apikey;
				datastreams.find({'group': group._id}).toArray(function(err, streams) {
					var nextStream = function() {
						var getStreamCallback = function getStreamCallback(httpCode, result) {
							rc.streams[result.name] = result;
							nextStream();
						};
						if (streams.length == 0) {
							res.send(200, rc);
							db.close();
						} else {
							var stream = streams.pop();
							getDataStream(db, {_id: stream._id}, getStreamCallback, parseInt(req.query.limit) || 5, req.query.type || 'average', true);	
						}
					};
					nextStream();
				});
			}
		});
	});
});

/* FIXME: test this function */
app.post('/group/:groupname', function(req, res) {
	// handle cascaded POST using only group's apikey
	/* example structure:
	 * POST: /<groupname>
	 * Body:
	 * { <k|key>: <apikey>, <a|at>: <time>, <s|streams>: { <streamname|sid>: value, <streamname|sid>: value } }
	 * OR: 
	 * { <k|key>: <apikey>, <from|f>: <time>, <s|streams>: {<streamname|sid>: [ {<<at|a>: <time> OR <offset|o>: <offset>>, <value|v>: value}, ... ], ...}}
	 */
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		var body = req.body,
			apikey = body.k || body.key,
			rc = { streams: 0, points: 0};
			
			console.log(body);
		if (err) {
			res.send(500, err.message);
		}
		var groups = db.collection('groups');
		groups.find({name: req.params.groupname}).toArray(function(err, grpresults) {
			if (grpresults.length == 0) {
				res.send(500, "No Such Group");
			} else {
				var group = grpresults[0];
console.log(group.apikey);
console.log(apikey);
				if (group.apikey == apikey) {
					// ok
					var at = body.a || body.at || body.f || body.from;
					if (!at) {
						at = new Date(); // fall back to now
					} else {
						at = new Date(at);
					}
					var streams = body.streams || body.s,
						streamnames = [];
					for (var i in streams) {
						if (streams.hasOwnProperty(i)) {
							streamnames.push(i);
						}
					}
					var datastreams = db.collection('datastreams');
					datastreams.find({group: group._id}).toArray(function(err, groupstreams) {
						var groupStreamNames = {};
						for (var i = 0; i < groupstreams.length; i++) {
							groupStreamNames[groupstreams[i].name] = groupstreams[i];
						}
						var processStream = function() {
							if (streamnames.length == 0) {
								res.send(200, rc);
							} else {
								rc.streams = rc.streams + 1;
								var streamName = streamnames.pop(),
									stream = streams[streamName],
									points = [];
								if ( groupStreamNames[streamName]) {
									// valid stream
									if (Array.isArray(stream)) {
										points = stream;
									} else {
										points.push(stream);
									}
							
									var processPoint = function() {
										if (points.length == 0) {
											processStream();
										} else {
											var point = points.pop(),
												pointAt = new Date(at.getTime()),
												pointValue = point.v || point.value;
											if (point.a || point.at) {
												pointAt = new Date(point.a || point.at);
											} else if (point.o || point.offset) {
												pointAt = new Date(at.getTime() + (point.o || point.offset));
											}
											// log data, update datastream value
											console.log(groupStreamNames[streamName]);
											navimonapi.addDataPoint(groupStreamNames[streamName], {at: pointAt, value: pointValue}, db, function() {
												processPoint();
											});
										}
									}
									processPoint();
								}
							}
						};
						processStream();
					});
				} else {
					res.send(404, "Invalid API Key For Stream Group");
				}
			}
		});
	});
});

var server = app.listen(3000, function() {
    console.log('Listening on port %d', server.address().port);
});
