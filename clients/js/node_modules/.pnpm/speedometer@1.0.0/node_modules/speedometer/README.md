# speedometer

Speed measurement in Javascript

```
npm install speedometer
```

## Usage

``` js
var speedometer = require('speedometer')
var fs = require('fs')

// Let's measure how fast we can read from /dev/urandom
var speed = speedometer()
var stream = fs.createReadStream('/dev/urandom')

stream.on('data', function(data) {
  // Simply call speed with the amount of bytes transferred
  var bytesPerSecond = speed(data.length)

  console.log(bytesPerSecond+' bytes/second')
})
```

You can always get the current speed by calling `speed()`.

Per default `speedometer` uses a 5 second buffer.
To change this simply pass another value to the constructor

``` js
var speed = speedometer(20) // uses a 20s buffer instead
```

## License

MIT
