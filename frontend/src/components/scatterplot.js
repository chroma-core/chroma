'use strict';
import createScatterplot from 'regl-scatterplot';

export default function scatterplot (opts) {
  var config = Object.assign({}, opts || {}, {
    backgroundColor: opts.backgroundColor || [1, 1, 1, 0],
    pixelRatio: opts.pixelRatio || Math.min(window.devicePixelRatio, 1.5),
  });

  var regl = config.regl;

  return new Promise(function (resolve, reject) {
  	try {
	    var canvas = config.canvas;

	    // draw operation
		const { width, height } = canvas.getBoundingClientRect();

		const scatterplot = createScatterplot({
		  canvas,
		  width,
		  height,
		  pointSize: 5,
		});
		console.log('scatterplot', scatterplot)

		const points = new Array(10000)
		  .fill()
		  .map(() => [-1 + Math.random() * 2, -1 + Math.random() * 2]);

		scatterplot.draw(points);
	        
	  } catch (e) {
	    if (regl) regl.destroy();
	    throw e;
	  }
	  resolve(Object.assign(config, {regl}))
  });

  
}

