const pkg = require('./package.json');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

// Remove a directory and its contents
const rmDir = dirPath => {
  if (fs.existsSync(dirPath)) {
    const files = fs.readdirSync(dirPath);
    for (const file of files) {
      const curPath = path.join(dirPath, file);
      fs.lstatSync(curPath).isDirectory() ? rmDir(curPath) : fs.unlinkSync(curPath);
    }
    fs.rmdirSync(dirPath);
  }
};

// Execute a shell command and return it as a Promise
const execCommand = cmd => {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout) => {
      if (error) {
        console.warn(error);
        return reject(error);
      }
      console.log(stdout);
      resolve(stdout);
    });
  });
};

// Initial logs
console.log(`Building JavaScript client v${pkg.version}...\n`);
console.log('Cleaning up old builds...\n');

// Commands to be used
const crossEnv = 'yarn cross-env';
const gulp = 'yarn gulp';

// Clean old builds
rmDir(path.join(__dirname, 'dist'));
rmDir(path.join(__dirname, 'lib'));
execCommand(`${gulp} cleanup`);

// Main build function
(async function build() {
  // Generate type definitions and compile code for different targets
  await Promise.all([
    execCommand(`${crossEnv} BUILD_TARGET=node ${gulp} compile`),
    execCommand(`${crossEnv} BUILD_TARGET=esm ${gulp} compile`),
    execCommand(`${gulp} generateRootJS`),
  ]);

  // Copy d.ts files
  console.log('Copy d.ts files:');
  await execCommand(`${gulp} dts`);
  
  // Update package json
  console.log('Update package json:');
  await execCommand(`${gulp} updatePackageJSON`);
}());
