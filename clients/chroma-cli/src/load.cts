// This module loads the platform-specific build of the addon on
// the current system. The supported platforms are registered in
// the `platforms` object below, whose entries can be managed by
// by the Neon CLI:
//
//   https://www.npmjs.com/package/@neon-rs/cli

module.exports = require('@neon-rs/load').proxy({
  platforms: {
    'win32-x64-msvc': () => require('@chroma-cli/win32-x64-msvc'),
    'darwin-x64': () => require('@chroma-cli/darwin-x64'),
    'darwin-arm64': () => require('@chroma-cli/darwin-arm64'),
    'linux-x64-gnu': () => require('@chroma-cli/linux-x64-gnu'),
    'linux-arm64-gnu': () => require('@chroma-cli/linux-arm64-gnu')
  },
  debug: () => require('../index.node')
});
