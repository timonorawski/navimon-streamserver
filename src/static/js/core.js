var core = {
	_defs: {
		data: "/json",
		target: '#loadTarget',
		initialPoints: 100,
		refreshPoints: 5,
		refreshTime: 10000,
		initialGranularity: "10m",
		refreshGranularity: "10s",
	},
	_vars: {
		refreshing: false,
	},
	_data: false,
	_charts: false,
	init: function() {
		core.getData(true);
		window.setInterval(core.getData, core._defs.refreshTime);
	},
	getData: function(initial) {
		if (core._vars.refreshing) {
			return;
		}
		core._vars.refreshing = true;
		var limit = core._defs.refreshPoints,
			granularity = core._defs.refreshGranularity;
		if (initial) {
			limit = core._defs.initialPoints;
			granularity = core._defs.initialGranularity;
		}
		var url = core._defs.data + "?limit=" + limit + "&granularity=" + granularity;
		$.ajax(url, {
			complete: function() {
				core._vars.refreshing = false;
			},
			success: function(data, status, xhr) {
				console.group(new Date());
				if (!core._data) {
					core._data = {};
				}
				for(var group in data) {
					if (!core._data[group]) {
						core._data[group] = {
							streams: {},
							hasStreamData: false
						};
					}
					var groupStreams = data[group].streams;
					for (var stream in groupStreams) {
						if (!core._data[group].streams[stream]) {
							core._data[group].streams[stream] = { map: {}, points:[] };
						}
						var streamCache = core._data[group].streams[stream];
						if (groupStreams[stream].datapoints) {
							for (var i = 0; i < groupStreams[stream].datapoints.length; i++) {
								var item =  groupStreams[stream].datapoints[i];
								if (!streamCache.map[item.at]) {
									console.log("adding datapoint value " + item.value + " to " + stream);
									var pt = {
										at: new Date(item.at),
										value: item.value,
									};
									if (item.stddev) {
										pt.stddevStart = item.value - item.stddev;
										pt.stddevEnd = item.value + item.stddev;
										pt.stddev = item.stddev;
									}
									streamCache.points.push(pt);
									core._data[group].hasStreamData = true;
									streamCache.map[item.at] = item;
								} else {
									streamCache.map[item.at].value = item.value;
									if (item.stddev) {
										pt.stddevStart = item.value - item.stddev;
										pt.stddevEnd = item.value + item.stddev;
										pt.stddev = item.stddev;
									}
								}
							}
						}
						/*if (groupStreams[stream].value) {
							core._data[group].hasStreamData = true;
							if (!streamCache.map[groupStreams[stream].at]) {
								console.log("adding current value " + groupStreams[stream].value + " to " + stream);
								streamCache.points.push({
									at: new Date(groupStreams[stream].at),
									value: groupStreams[stream].value
								});
								streamCache.map[groupStreams[stream].at] = true;
							}
						}*/
						streamCache.points.sort(function(a,b) {
							if (a.at.getTime() < b.at.getTime()) {
								return -1
							} else if (a.at.getTime() == b.at.getTime()) {
								return 0;
							} else{
								return 1;
							}
						})
					}
					if (!core._data[group].hasStreamData) {
						delete core._data[group];
					}
				}
				core.drawCharts();
				console.groupEnd();
			}
		});
	},
	drawChart: function(group, stream) {
		// init chart
		var chartFunc = this._chartFuncs.serial;
		/*switch(stream) {
		case "heading":
		case "magheading":
			chartFunc = this._chartFuncs.polar;
			break;
			
		}*/
		this._charts[group][stream].chart = chartFunc(group+"_"+stream, this._data[group].streams[stream].points);
	},
	drawCharts: function() {
		if (!this._charts) {
			this._charts = {};
			for (var group in this._data) {
				if (this._data.hasOwnProperty(group)) {
					this._charts[group] = {};
					$(this._defs.target).append('<div class="chartGroup" id="' + group + '"><h1>' + group + '</h1></div>');
					var chartGroup = $('#' + group);
					for (var stream in this._data[group].streams) {
						if (this._data[group].streams.hasOwnProperty(stream) && this._data[group].streams[stream].points.length > 0) {
							this._charts[group][stream] = {};
							chartGroup.append('<div class="chartWrap"><h2>'+stream+'</h2><div class="chart" id="' + group + "_" + stream + '"></div></div>');
							core.drawChart(group, stream);
/*							$('#'+group + "_" + stream).on('click', function() {
								core.drawChart(group, stream);
							})*/
						}
					}
				}
			}
		} else {
			for (var group in this._charts) {
				if (this._charts.hasOwnProperty(group)) {
					for (var stream in this._charts[group]) {
						if (this._charts[group].hasOwnProperty(stream)) {
							this._charts[group][stream].chart.validateData();
						}
					}
				}
			}
		}
	},
	_chartFuncs: {
		serial: function(id, data) {
			return AmCharts.makeChart(id, {
			    "type": "serial",
			    "theme": "none",
			    "marginLeft": 0,
			    "pathToImages": "http://www.amcharts.com/lib/3/images/",
			    "dataProvider": data,
			    "valueAxes": [{
			        "axisAlpha": 0,
			        "inside": true,
			        "position": "left",
			        "ignoreAxisWidth": true
			    }],
			    "graphs": [
					{
			            "alphaField": "alpha",
			            "dashLengthField": "dashLengthColumn",
				        "balloonText": "Standard Deviation<br><b><span style='font-size:14px;'>[[stddev]]</span></b>",
			            "fillAlphas": 1,
			            "title": "Standard Deviation",
			            "type": "column",
						"openField": "stddevStart",
			            "valueField": "stddevEnd"
			        },
					{
				        "balloonText": "[[category]]<br><b><span style='font-size:14px;'>[[value]]</span></b>",
				        "bullet": "round",
				        "bulletSize": 6,
				        "lineColor": "#d1655d",
				        "lineThickness": 2,
				        "negativeLineColor": "#637bb6",
				        "type": "smoothedLine",
				        "valueField": "value"
				    }
				],
			    "chartScrollbar": {},
			    "chartCursor": {
			        "categoryBalloonDateFormat": "YYYY-MM-DD HH:NN:SS",
			        "cursorAlpha": 0,
			        "cursorPosition": "mouse"
			    },
			    //"dataDateFormat": "YYYY-MM-DD HH:NN:SS",
			    "categoryField": "at",
			    "categoryAxis": {
			        "minPeriod": "ss",
			        "parseDates": true,
			        "minorGridAlpha": 0.1,
			        "minorGridEnabled": true
			    }
			});
		},
		polar: function(id, data) {
			return AmCharts.makeChart("chartdiv", {
			    "type": "radar",
			    "theme": "none",
			    "dataProvider": data,
			    "valueAxes": [{
			        "gridType": "circles",
			        "minimum": 0,
			        "autoGridCount": false,
			        "axisAlpha": 0.2,
			        "fillAlpha": 0.05,
			        "fillColor": "#FFFFFF",
			        "gridAlpha": 0.08,
			        "guides": [{
			            "angle": 225,
			            "fillAlpha": 0.3,
			            "fillColor": "#0066CC",
			            "tickLength": 0,
			            "toAngle": 315,
			            "toValue": 14,
			            "value": 0,
						"lineAlpha": 0,

			        }, {
			            "angle": 45,
			            "fillAlpha": 0.3,
			            "fillColor": "#CC3333",
			            "tickLength": 0,
			            "toAngle": 135,
			            "toValue": 14,
			            "value": 0,
						"lineAlpha": 0,
			        }],
			        "position": "left"
			    }],
			    "startDuration": 1,
			    "graphs": [{
			        "balloonText": "[[category]]: [[at]] time",
			        "bullet": "round",
			        "fillAlphas": 0.3,
			        "valueField": "at"
			    }],
			    "categoryField": "value"   
			});
		}
	}
};

$(document).ready(core.init);