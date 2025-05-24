module.exports = {
  // specify additional options here, especially http(s)
  // see https://nodejs.org/api/tls.html#tls_tls_connect_options_callback for specifics
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
  auth: {
    username: 'yourusername',
    password: 'yourpassword'
  },
  strictSSL: false,
  followRedirect: false,
  headers: {
    'x-custom': 'headers'
  },
  // optional default resources if not specified in command args
  resources: ['http://foo/bar', 'http://cat/dog']
};
