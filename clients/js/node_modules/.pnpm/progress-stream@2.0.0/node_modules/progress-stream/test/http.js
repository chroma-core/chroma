var progress = require('../index');
var http = require('http');
var fs = require('fs');
var log = require('single-line-log').stdout;
var numeral = require('numeral');

var str = progress({
	drain: true,
	time: 100,
	speed: 20
});
str.on('progress', function(progress) {
	log('Running: '+numeral(progress.runtime).format('00:00:00')+' ('+numeral(progress.transferred).format('0 b')+')\n'+
		'Left:    '+numeral(progress.eta).format('00:00:00')+' ('+numeral(progress.remaining).format('0 b')+')\n'+
		numeral(progress.speed).format('0.00b')+'/s '+Math.round(progress.percentage)+'%');
});

var options = {
	method: 'GET',
	host: 'cachefly.cachefly.net',
	path: '/10mb.test',
	headers: {
		'user-agent': 'testy test'
	}
};
http.request(options, function(response) {
	response.pipe(str);
}).end();

console.log('progress-stream using http module - downloading 10 MB file');
