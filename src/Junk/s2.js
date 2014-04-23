var express = require('express'),
	app = express(),
	mongodb = require('mongodb'),
	uuid = require('node-uuid'),
	format = require('util').format,
	fs = require('fs'),
	navimonapi = require("./modules/api.js"),
	carrier = require('carrier');


app.use(express.bodyParser());

var getDataStream = function getDataStream(db, streamid, callback, limit, sanitize) {
	if (!limit) limit = 5;
    var collection = db.collection('datastreams');
	streamid = new mongodb.ObjectID(streamid);
    collection.find({"_id": streamid}).toArray(function(err, results) {
		if (results.length > 0) {
			var datapoints = db.collection("datapoints"),
				doc = results[0],
				cursor = datapoints.find({"streamid": streamid}, {'value': true, 'at': true});
			if (sanitize) {
				delete doc.apikey;
				delete doc.user;
			}
			cursor.sort({'at': -1}).limit(limit).toArray(function(err, results) {
				if (!results) {
					results = [];
				}
				for (var i = 0; i < results.length; i++) {
					var point = results[i];
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

var deletepointswithnotime = function() {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		var datapoints = db.collection("datapoints");
		datapoints.find().toArray(function(err, results) {
			for ( var i = 0; i < results.length; i++) {
				if (results[i].at == undefined) {
					datapoints.remove(results[i], {justOne: true}, function(err, num) {
						console.log("Remove complete");
					});
					console.log('removed');
					console.log(results[i]._id);
				}
			}
			console.log("done, processed: " + results.length);
		})
	});
}
//deletepointswithnotime();
var aggregate = function() {
	var bucketSize = 60 * 1000; // seconds
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		var collection = db.collection('datastreams');
	    collection.find({}).toArray(function(err, streams) {
			var processNext = function() {
				var stream = streams.pop();
				if (stream) {
					console.log("PROCESSING STREAM");
					console.log(stream);
					var datapoints = db.collection("datapoints");
					datapoints.find({'streamid':stream._id}, {}).sort({"at":1}).toArray(function(err, results) {
						var buckets = {},
							bucketsToSave = [],
							docsToFix = [];
						for (var i = 0; i < results.length; i++) {
							if (results[i].type == "average") {
								results[i].count = 0;
								results[i].value = 0;
								var tm = results[i].at;
								if (!buckets[tm.getFullYear()]) {
									buckets[tm.getFullYear()] = {};
								}
								if (!buckets[tm.getFullYear()][tm.getMonth()]) {
									buckets[tm.getFullYear()][tm.getMonth()] = {};
								}
								if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()]) {
									buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()] = {};
								}
								if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()]) {
									buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()] = {};
								}
								if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()][tm.getMinutes()]) {
									bucketsToSave.push(bucket);
									buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()][tm.getMinutes()] = results[i];
								}
							}
						}
						for (var i = 0; i < results.length; i++) {
							var tm = results[i].at;
							if (!results[i].type) {
								results[i].type = "raw";
								docsToFix.push(results[i]);
							}
							if (results[i].type == "average") {
								// ignore averages in averaging
								continue;
							}
							if (!buckets[tm.getFullYear()]) {
								buckets[tm.getFullYear()] = {};
							}
							if (!buckets[tm.getFullYear()][tm.getMonth()]) {
								buckets[tm.getFullYear()][tm.getMonth()] = {};
							}
							if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()]) {
								buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()] = {};
							}
							if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()]) {
								buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()] = {};
							}
							if (!buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()][tm.getMinutes()]) {
								var bucket = {
									streamid: stream._id,
									type: "average",
									size: "1m",
									count: 0,
									value: 0,
									at: new Date(tm.getTime())
								};
								bucket.at.setSeconds(0);
								bucket.at.setMilliseconds(0);
								bucketsToSave.push(bucket);
								buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()][tm.getMinutes()] = bucket;
							}
							var minuteBucket = buckets[tm.getFullYear()][tm.getMonth()][tm.getDate()][tm.getHours()][tm.getMinutes()];
							minuteBucket.value = ((minuteBucket.value * minuteBucket.count) + results[i].value) / (minuteBucket.count + 1);
							minuteBucket.count = minuteBucket.count + 1;
							//console.log(tm.getTime());
						}
						//console.log(JSON.stringify(buckets));
						var saveNextBucket = function() {
							var doc = docsToFix.pop();
							if (doc) {
								datapoints.save(doc, function() {
									saveNextBucket();
								});
							} else {
								var bucket = bucketsToSave.pop();
								if (bucket) {
									if (bucket._id) {
									    datapoints.save(bucket, function(err, docs) {
											console.log('updated 1m average for ' + bucket.at + " in stream " + bucket.streamid);
											saveNextBucket();
									    });
									} else {
									    datapoints.insert(bucket, function(err, docs) {
											console.log('added 1m average for ' + bucket.at + " in stream " + bucket.streamid);
											saveNextBucket();
									    });
									}
								} else {
									processNext();
								}
							}
						}
						saveNextBucket();
//						processNext();
					});
				} else {
					process.exit();
				}
			}
			processNext();
		});
	});
};
//aggregate();
var lineReader = require('line-reader');
var reimportData = function() {
	mongodb.MongoClient.connect('mongodb://127.0.0.1:27017/DataServer', function(err, db) {
		var datastreams = db.collection('datastreams');
		datastreams.find({}).toArray(function(err, streams) {
			var streamMap = {};
			for (var i = 0; i < streams.length; i++) {
				console.log(streams[i]);
				streamMap[streams[i].name] = streams[i];
			}
			console.log(streamMap);
			fs.readdir("databackup", function(err, files) {
				var nextFile = function() {
					if (files.length == 0) {
						console.log("DONE");
						process.exit();
						return;
					}
					var file = files.pop();
					console.log(file);
					var streamName = file.split(".")[0];
					var nLines = 0,
						entries = [];
					var processData = function() {
						var processing = true;
						var processNext = function() {
							var point = entries.pop();
							if (entries.length == 0) {
								processing = false;
							}
//							navimonapi.addDataPoint(streamMap[streamName], point, db, function() {}, false);
						};
						setTimeout(function() {
							if (entries.length == 0 && !processing) {
								nextFile();
							} else {
								processNext();
							}
						}, 1);
					};
					lineReader.eachLine("databackup/"+file, function(line, last) {
						//console.log(line);
						nLines++;
						// do whatever you want with line...
						var parts = line.split(","),
							date = new Date(parts[0]),
							data = parts[1];
						if (parseFloat(data) != NaN) {
							data = parseFloat(data);
						}
						var entry = {at: date, value: data};
						entries.push(entry);
						if(last){
							console.log(nLines);
							processData();
						// or check if it's the last one
						}
					});

				};
				nextFile();
			});
		});
	});
};

reimportData();


var server = app.listen(3001, function() {
    console.log('Listening on port %d', server.address().port);
});
