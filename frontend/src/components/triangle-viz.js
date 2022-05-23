'use strict';

var createREGL = require('regl');

function initialize (opts) {
  var config = Object.assign({}, opts || {}, {
    backgroundColor: opts.backgroundColor || [1, 1, 1, 0],
    pixelRatio: opts.pixelRatio || Math.min(window.devicePixelRatio, 1.5),
  });

  return new Promise(function (resolve, reject) {
    createREGL({
      canvas: (config.canvas),
      pixelRatio: config.pixelRatio,
      extensions: ['OES_texture_float'],
      optionalExtensions: [
        'OES_texture_float_linear',
        'OES_texture_half_float',
        'OES_texture_half_float_linear',
        'WEBGL_depth_texture',
      ],
      attributes: {
        antialias: false,
        alpha: true,
        depthStencil: true,
      },
      onDone: (err, regl) => {
        if (err) return reject(err)

        // Clear to white ASAP and don't block
        regl.clear({color: [1, 1, 1, 1]})

        requestAnimationFrame(() => {
          resolve(Object.assign(config, {regl}))
        })
      }
    });
  });
}

module.exports = function triangleViz (opts) {
  return initialize(opts)
    .then(run);
}

function run (config) {
  var regl = config.regl;

  try {
    var canvas = regl._gl.canvas;

    // This clears the color buffer to black and the depth buffer to 1
    regl.clear({
        color: [0, 0, 0, 1],
        depth: 1
    })
    
    // In regl, draw operations are specified declaratively using. Each JSON
    // command is a complete description of all state. This removes the need to
    // .bind() things like buffers or shaders. All the boilerplate of setting up
    // and tearing down state is automated.
    regl({
        // In a draw call, we can pass the shader source code to regl
        frag: `
        precision mediump float;
        uniform vec4 color;
        void main () {
        gl_FragColor = color;
        }`,
        vert: `
        precision mediump float;
        attribute vec2 position;
        void main () {
        gl_Position = vec4(position, 0, 1);
        }`,
        attributes: {
        position: [
            [-1, 0],
            [0, -1],
            [1, 1]
        ]
        },
        uniforms: {
        color: [1, 0, 0, 1]
        },
        count: 3
    })()
        
  } catch (e) {
    if (regl) regl.destroy();
    throw e;
  }
  return config;
}