# progress-stream

Read the progress of a stream. Supports speed and eta.

Gets the length of the stream automatically if you're using the request or http module. You can also pass the length on initiation. Progress-stream will also check to see if the stream already has a length property.

	npm install progress-stream

## Usage

This example copies a large file, and prints out the percentage, speed and remaining every 100ms.

```js
var progress = require('progress-stream');
var fs = require('fs');

var stat = fs.statSync(filename);
var str = progress({
	length: stat.size,
	time: 100 /* ms */
});

str.on('progress', function(progress) {
	console.log(progress);

	/*
	{
		percentage: 9.05,
		transferred: 949624,
		length: 10485760,
		remaining: 9536136,
		eta: 42,
		runtime: 3,
		delta: 295396,
		speed: 949624
	}
	*/
});

fs.createReadStream(filename)
	.pipe(str)
	.pipe(fs.createWriteStream(output));
```

## Methods

### progress([options], [onprogress])

You can instantiate in two ways:

``` js
var str = progress({time:100});
str.on('progress', function(progress) { ... });
```

or inline the progress listener

``` js
var str = progress({time:100}, function(progress) { ... });
```

## Properties

### .progress()

You can get the progress from the progress function.

``` js
var str = progress({time:100});

console.log(str.progress());

/*
{
	percentage: 9.05,
	transferred: 949624,
	length: 10485760,
	remaining: 9536136,
	eta: 10,
	runtime: 0,
	delta: 295396,
	speed: 949624
}
*/
```

## Events

### on('progress', function(progress) { ... })

``` js
var str = progress({time:100});
str.on('progress', function(progress) { ... });
```

## Options

### time(integer)

Sets how often progress events are emitted in ms. If omitted then the default is to do so every time a chunk is received.

### speed(integer)

Sets how long the speedometer needs to calculate the speed. Defaults to 5 sec.

### length(integer)

If you already know the length of the stream, then you can set it. Defaults to 0.

### drain(boolean)

In case you don't want to include a readstream after progress-stream, set to true to drain automatically. Defaults to false.

### transferred(integer)

If you want to set the size of previously downloaded data. Useful for a resumed download.

## Examples

### Using the request module

This example uses request to download a 100 MB file, and writes out the percentage every second.

You can also find an example in `test/request.js`.

``` js
var progress = require('progress-stream');
var req = require('request');
var fs = require('fs');

var str = progress({
	time: 1000
});

str.on('progress', function(progress) {
	console.log(Math.round(progress.percentage)+'%');
});

req('http://cachefly.cachefly.net/100mb.test', { headers: { 'user-agent': 'test' }})
	.pipe(str)
	.pipe(fs.createWriteStream('test.data'));
```

### Using the http module

In `test/http.js` it's shown how to do it with the http module.


## Methods


### `setLength(newLength)`

Sometimes, you don't know how big a stream is right away (e.g. multipart file uploads).  You might find out after a few chunks have already passed through the stream, seconds or even minutes later.  In this case, you can use the `setLength` method to recalculate the relevant tracked progress data.

```js
var str = progress({});
someFickleStreamInstance.pipe(str).pipe(fs.createWriteStream('test.data'));

someFickleStreamInstance.on('conviction', function nowIKnowMyLength (actualLength) {
  str.setLength(actualLength);
});
```
