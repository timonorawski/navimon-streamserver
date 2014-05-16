var mongodb = require('mongodb'),
	uuid = require('node-uuid'),
	format = require('util').format,
	fs = require('fs');

var doAverageBucket = function(sizeInSeconds, stream, point, mongo, callback) {
	var datapoints = mongo.collection('datapoints'),
		averageMinutes = Math.floor(sizeInSeconds/60),
		averageTime = new Date(point.at.getTime()),
		averagePeriod = "1m",
		streamLastUpdate = stream.lastUpdate,
		streamid = stream._id;
	try {
		streamid = new mongodb.ObjectID(streamid);
	} catch(e) {}
	averageTime.setTime(averageTime.getTime() - (averageTime.getTime()%(sizeInSeconds*1000)));
	if (sizeInSeconds < 60) {
		averagePeriod = "" + sizeInSeconds + "s";
	} else if (averageMinutes != 60 && averageMinutes != 1440) {
		averagePeriod = "" + averageMinutes + "m";
	} else if (averageMinutes % 60 == 0) {
		averagePeriod = "" + Math.floor(averageMinutes/60) + "h";
	} else if (averageMinutes%1440 == 0) {
		averagePeriod = "" + Math.floor(averageMinutes/1440) + "d";
	}
	datapoints.find({at:averageTime, type:"average", streamid: streamid, averagePeriod: averagePeriod}).toArray(function(err, results) {
		if (results.length != 0) {
			var avgPoint = results[0],
				avg = ((avgPoint.value * avgPoint.datapoints) + point.value) / (avgPoint.datapoints + 1);
			avgPoint.datapoints = parseFloat(avgPoint.datapoints) + 1;
			avgPoint.value = avg;
			avgPoint.sum = parseFloat(avgPoint.sum) + point.value;
			avgPoint.sumsq = parseFloat(avgPoint.sumsq) + (point.value*point.value);
			avgPoint.stddev = Math.sqrt((avgPoint.datapoints*avgPoint.sumsq)-(avgPoint.sum*avgPoint.sum))/avgPoint.datapoints;
			datapoints.update({_id: avgPoint._id}, {$set:{datapoints: avgPoint.datapoints, value: avgPoint.value, sum: avgPoint.sum, sumsq: avgPoint.sumsq, stddev: avgPoint.stddev}}, {}, callback);
		} else {
			var newpoint = {
				at: averageTime,
				streamid: streamid,
				type: "average",
				datapoints: 1,
				averagePeriod: averagePeriod,
				value: point.value,
				sum: point.value,
				sumsq: point.value*point.value,
			};
			newpoint.stddev = Math.sqrt( (newpoint.datapoints*newpoint.sumsq)-(newpoint.sum*newpoint.sum))/newpoint.datapoints;
			datapoints.insert(newpoint, callback);
		}
	});
};
exports.addDataPoint = function(stream, point, mongo, callback, writeLog) {
	if (typeof writeLog == undefined) {
		writeLog = true;
	}
	writeLog = true;
	
	var datastreams = mongo.collection('datastreams'),
		datapoints = mongo.collection('datapoints');
	var streamLastUpdate = stream.lastUpdate;
	var streamid = stream._id;
	try {
		streamid = new mongodb.ObjectID(stream._id);
	} catch(e){}
	datastreams.update({"_id": streamid}, {$set:{lastUpdate: point.at, currentValue: point.value}}, {}, function(err){});

	var cb = function() {
		var date = new Date();
		/* write csv file for raw updates */
		if (writeLog) {
			fs.appendFile('data/' + stream._id + "_" + date.getFullYear() +"-" + (date.getMonth()+1) + "-"+ (date.getDate())+ ".csv", "" + point.at.toISOString() + "," + point.value + "\n",
			function (err) {
				callback();
			});
		} else {
			callback();
		}
	};
	doAverageBucket(10, stream, point, mongo, function() {
		doAverageBucket(30, stream, point, mongo, function() {
			doAverageBucket(60, stream, point, mongo, function() {
				doAverageBucket(300, stream, point, mongo, function() {
					doAverageBucket(600, stream, point, mongo, function() {
						doAverageBucket(3600, stream, point, mongo, function() {
							doAverageBucket(86400, stream, point, mongo, cb);
						});
					});
				});
			});
		});
	});
};