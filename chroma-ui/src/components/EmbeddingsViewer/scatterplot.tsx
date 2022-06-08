// @ts-nocheck

'use strict';
import createScatterplot from 'regl-scatterplot';

export default function scatterplot (points, colorsScale, opts) {
  var config = Object.assign({}, opts || {}, {
    backgroundColor: opts.backgroundColor || [1, 1, 1, 0],
    pixelRatio: opts.pixelRatio || Math.min(window.devicePixelRatio, 1.5),
  });

  return new Promise(function (resolve, reject) {
  	try {
	    var canvas = config.canvas;

		const { width, height } = canvas.getBoundingClientRect();
		const scatterplot = createScatterplot({
		  canvas,
		  width,
		  height,
		  pointSize: 7
		});
		scatterplot.set({ backgroundColor: '#F3F5F6' }); 
		scatterplot.draw(points);

		const getOpacityRange = () =>
			Array(10)
        .fill()
        .map((x, i) => ((i + 1) / 10));

		scatterplot.set({ opacity: getOpacityRange() });
		scatterplot.set({ colorBy: 'valueW', opacityBy: 'valueZ', pointColor: colorsScale, pointOutlineWidth: 5,  });
		scatterplot.subscribe('select', opts.selectHandler);
		scatterplot.subscribe('deselect', opts.deselectHandler);
		config['scatterplot'] = scatterplot
		config['regl'] = scatterplot.get('regl')
	        
	  } catch (e) {
	    if (regl) regl.destroy();
	    throw e;
	  }
	  resolve(config)
  });

  
}

