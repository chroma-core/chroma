var progress = require('../index');
var req = require('request');
var fs = require('fs');
var log = require('single-line-log').stdout;
var numeral = require('numeral');

var str = progress({
	drain: true,
	time: 100
}, function(progress) {
	log('Running: '+numeral(progress.runtime).format('00:00:00')+' ('+numeral(progress.transferred).format('0 b')+')\n'+
		'Left:    '+numeral(progress.eta).format('00:00:00')+' ('+numeral(progress.remaining).format('0 b')+')\n'+
		numeral(progress.speed).format('0.00b')+'/s '+Math.round(progress.percentage)+'%');
});

req('http://cachefly.cachefly.net/10mb.test', {
	headers: { 'user-agent': 'test' }
}).pipe(str);

console.log('progress-stream using request module - downloading 10 MB file');
