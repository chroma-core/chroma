# wait-on - wait for files, ports, sockets, http(s) resources

wait-on is a cross-platform command line utility which will wait for files, ports, sockets, and http(s) resources to become available (or not available using reverse mode). Functionality is also available via a Node.js API. Cross-platform - runs everywhere Node.js runs (linux, unix, mac OS X, windows)

wait-on will wait for period of time for a file to stop growing before triggering availability which is good for monitoring files that are being built. Likewise wait-on will wait for period of time for other resources to remain available before triggering success.

For http(s) resources wait-on will check that the requests are returning 2XX (success) to HEAD or GET requests (after following any redirects).

wait-on can also be used in reverse mode which waits for resources to NOT be available. This is useful in waiting for services to shutdown before continuing. (Thanks @skarbovskiy for adding this feature)

[![CI](https://github.com/jeffbski/wait-on/actions/workflows/node.js.yml/badge.svg)](https://github.com/jeffbski/wait-on/actions/workflows/node.js.yml)

## Installation

Latest wait-on version 7+ requires Node.js 12+
wait-on versions 4-6 requires Node.js 10+

(Node.js v8 users can use wait-on@5.3.0, v4 users can still use wait-on@2.1.2, and older Node.js
engines, use wait-on@1.5.4)

```bash
npm install wait-on # local version
OR
npm install -g wait-on # global version
```

## Usage

Use from command line or using Node.js programmatic API.

### CLI Usage

Assuming NEXT_CMD is the command to run when resources are available, then wait-on will wait and then exit with a successful exit code (0) once all resources are available, causing NEXT_CMD to be run.

wait-on can also be used in reverse mode, which waits for resources to NOT be available. This is useful in waiting for services to shutdown before continuing. (Thanks @skarbovskiy for adding)

If wait-on is interrupted before all resources are available, it will exit with a non-zero exit code and thus NEXT_CMD will not be run.

```bash
wait-on file1 && NEXT_CMD # wait for file1, then exec NEXT_CMD
wait-on f1 f2 && NEXT_CMD # wait for both f1 and f2, the exec NEXT_CMD
wait-on http://localhost:8000/foo && NEXT_CMD # wait for http 2XX HEAD
wait-on https://myserver/foo && NEXT_CMD # wait for https 2XX HEAD
wait-on http-get://localhost:8000/foo && NEXT_CMD # wait for http 2XX GET
wait-on https-get://myserver/foo && NEXT_CMD # wait for https 2XX GET
wait-on tcp:4000 && NEXT_CMD # wait for service to listen on a TCP port
wait-on socket:/path/mysock # wait for service to listen on domain socket
wait-on http://unix:/var/SOCKPATH:http://server/a/foo # wait for http HEAD on domain socket
wait-on http-get://unix:/var/SOCKPATH:http://server/a/foo # wait for http GET on domain socket
```

```
Usage: wait-on {OPTIONS} resource [...resource]

Description:

     wait-on is a command line utility which will wait for files, ports,
     sockets, and http(s) resources to become available (or not available
     using reverse flag). Exits with  success code (0) when all resources
     are ready. Non-zero exit code if interrupted or timed out.

     Options may also be specified in a config file (js or json). For
     example --config configFile.js would result in configFile.js being
     required and the resulting object will be merged with any
     command line options before wait-on is called. See exampleConfig.js

     In shell combine with && to conditionally run another command
     once resources are available. ex: wait-on f1 && NEXT_CMD

     resources types are defined by their prefix, if no prefix is
     present, the resource is assumed to be of type 'file'. Resources
     can also be provided in the config file.

     resource prefixes are:

       file:      - regular file (also default type). ex: file:/path/to/file
       http:      - HTTP HEAD returns 2XX response. ex: http://m.com:90/foo
       https:     - HTTPS HEAD returns 2XX response. ex: https://my/bar
       http-get:  - HTTP GET returns 2XX response. ex: http://m.com:90/foo
       https-get: - HTTPS GET returns 2XX response. ex: https://my/bar
       tcp:       - TCP port is listening. ex: 1.2.3.4:9000 or foo.com:700
       socket:    - Domain Socket is listening. ex: socket:/path/to/sock
                    For http over socket, use http://unix:SOCK_PATH:URL_PATH
                    like http://unix:/path/to/sock:http://server/foo/bar or
                         http-get://unix:/path/to/sock:http://server/foo/bar

Standard Options:

 -c, --config

  js or json config file, useful for http(s) options and resources

 -d, --delay

  Initial delay before checking for resources in ms, default 0

 --httpTimeout

  Maximum time in ms to wait for an HTTP HEAD/GET request, default 0
  which results in using the OS default

-i, --interval

  Interval to poll resources in ms, default 250ms

 -l, --log

  Log resources begin waited on and when complete or errored

 -r, --reverse

  Reverse operation, wait for resources to NOT be available

 -s, --simultaneous

  Simultaneous / Concurrent connections to a resource, default Infinity
  Setting this to 1 would delay new requests until previous one has completed.
  Used to limit the number of connections attempted to a resource at a time.

 -t, --timeout

  Maximum time in ms to wait before exiting with failure (1) code,
  default Infinity
  Use postfix 'ms', 's', 'm' or 'h' to change the unit.

  --tcpTimeout

  Maximum time in ms for tcp connect, default 300ms
  Use postfix 'ms', 's', 'm' or 'h' to change the unit.

  --httpTimeout

  Maximum time to wait for the HTTP request, default Infinity
  Use postfix 'ms', 's', 'm' or 'h' to change the unit.

 -v, --verbose

  Enable debug output to stdout

 -w, --window

  Stability window, the time in ms defining the window of time that
  resource needs to have not changed (file size or availability) before
  signalling success, default 750ms. If less than interval, it will be
  reset to the value of interval. This is only used for files, other
  resources are considered available on first detection.

 -h, --help

  Show this message
```

### Node.js API usage

```javascript
var waitOn = require('wait-on');
var opts = {
  resources: [
    'file1',
    'http://foo.com:8000/bar',
    'https://my.com/cat',
    'http-get://foo.com:8000/bar',
    'https-get://my.com/cat',
    'tcp:foo.com:8000',
    'socket:/my/sock',
    'http://unix:/my/sock:http://server/my/url',
    'http-get://unix:/my/sock:http://server/my/url'
  ],
  delay: 1000, // initial delay in ms, default 0
  interval: 100, // poll interval in ms, default 250ms
  simultaneous: 1, // limit to 1 connection per resource at a time
  timeout: 30000, // timeout in ms, default Infinity
  tcpTimeout: 1000, // tcp timeout in ms, default 300ms
  window: 1000, // stabilization time in ms, default 750ms

  // http options
  ca: [
    /* strings or binaries */
  ],
  cert: [
    /* strings or binaries */
  ],
  key: [
    /* strings or binaries */
  ],
  passphrase: 'yourpassphrase',
  proxy: false /* OR proxy config as defined in axios.
  If not set axios detects proxy from env vars http_proxy and https_proxy
  https://github.com/axios/axios#config-defaults
  {
    host: '127.0.0.1',
    port: 9000,
    auth: {
      username: 'mikeymike',
      password: 'rapunz3l'
    }
  } */,
  auth: {
    user: 'theuser', // or username
    pass: 'thepassword' // or password
  },
  strictSSL: false,
  followRedirect: true,
  headers: {
    'x-custom': 'headers'
  },
  validateStatus: function (status) {
    return status >= 200 && status < 300; // default if not provided
  }
};

// Usage with callback function
waitOn(opts, function (err) {
  if (err) {
    return handleError(err);
  }
  // once here, all resources are available
});

// Usage with promises
waitOn(opts)
  .then(function () {
    // once here, all resources are available
  })
  .catch(function (err) {
    handleError(err);
  });

// Usage with async await
try {
  await waitOn(opts);
  // once here, all resources are available
} catch (err) {
  handleError(err);
}
```

waitOn(opts, [cb]) - function which triggers resource checks

- opts.resources - array of string resources to wait for. prefix determines the type of resource with the default type of `file:`
- opts.delay - optional initial delay in ms, default 0
- opts.interval - optional poll resource interval in ms, default 250ms
- opts.log - optional flag which outputs to stdout, remaining resources waited on and when complete or errored
- opts.resources - optional array of string resources to wait for if none are specified via command line
- opts.reverse - optional flag to reverse operation so checks are for resources being NOT available, default false
- opts.simultaneous - optional count to limit concurrent connections per resource at a time, setting to 1 waits for previous connection to succeed, fail, or timeout before sending another, default infinity
- opts.timeout - optional timeout in ms, default Infinity. Aborts with error.
- opts.tcpTimeout - optional tcp timeout in ms, default 300ms
- opts.verbose - optional flag which outputs debug output, default false
- opts.window - optional stabilization time in ms, default 750ms. Waits this amount of time for file sizes to stabilize or other resource availability to remain unchanged.
- http(s) specific options, see https://nodejs.org/api/tls.html#tls_tls_connect_options_callback for specific details

  - opts.ca: [ /* strings or binaries */ ],
  - opts.cert: [ /* strings or binaries */ ],
  - opts.key: [ /* strings or binaries */ ],
  - opts.passphrase: 'yourpassphrase',
  - opts.proxy: undefined, false, or object as defined in axios. Default is undefined. If not set axios detects proxy from env vars http_proxy and https_proxy. https://github.com/axios/axios#config-defaults

```js
  // example proxy object
  {
    host: '127.0.0.1',
    port: 9000,
    auth: {
      username: 'mikeymike',
      password: 'rapunz3l'
    }
  }
```

- opts.auth: { user, pass }
- opts.strictSSL: false,
- opts.followRedirect: false, // defaults to true
- opts.headers: { 'x-custom': 'headers' },

- cb(err) - if err is provided then, resource checks did not succeed

## Goals

- simple command line utility and Node.js API for waiting for resources
- wait for files to stabilize
- wait for http(s) resources to return 2XX in response to HEAD request
- wait for http(s) resources to return 2XX in response to GET request
- wait for services to be listening on tcp ports
- wait for services to be listening on unix domain sockets
- configurable initial delay, poll interval, stabilization window, timeout
- command line utility returns success code (0) when resources are availble
- command line utility that can also wait for resources to not be available using reverse flag. This is useful for waiting for services to shutdown before continuing.
- cross platform - runs anywhere Node.js runs (linux, unix, mac OS X, windows)

## Why

I frequently need to wait on build tasks to complete or services to be available before starting next command, so this project makes that easier and is portable to everywhere Node.js runs.

## Get involved

If you have input or ideas or would like to get involved, you may:

- contact me via twitter @jeffbski - <http://twitter.com/jeffbski>
- open an issue on github to begin a discussion - <https://github.com/jeffbski/wait-on/issues>
- fork the repo and send a pull request (ideally with tests) - <https://github.com/jeffbski/wait-on>

## License

- [MIT license](http://github.com/jeffbski/wait-on/raw/master/LICENSE)
