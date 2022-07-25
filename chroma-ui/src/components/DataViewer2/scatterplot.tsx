// @ts-nocheck
import createScatterplot from 'regl-scatterplot'

export default function scatterplot(points, colorsScale, opts) {
  var config = Object.assign({}, opts || {}, {
    backgroundColor: opts.backgroundColor || [1, 1, 1, 0],
    pixelRatio: opts.pixelRatio || Math.min(window.devicePixelRatio, 1.5),
    distance: opts.distance || 1,
    target: opts.target || [0, 0],
  })

  return new Promise(function (resolve, reject) {
    try {
      var canvas = config.canvas

      const scatterplotInstance = createScatterplot({
        canvas,
        width: 'auto',
        height: 'auto',
        pointSize: 10,
        showReticle: true,
        reticleColor: [1, 1, 0.878431373, 0],
      })
      scatterplotInstance.set({ backgroundColor: '#ffffff' })
      scatterplotInstance.draw(points)
      scatterplotInstance.set({ opacity: [0, 1] })
      scatterplotInstance.set({ colorBy: 'valueW', opacityBy: 'valueZ', pointColor: colorsScale, pointOutlineWidth: 5, })
      scatterplotInstance.subscribe('select', opts.selectHandler)
      scatterplotInstance.subscribe('deselect', opts.deselectHandler)

      var defaultDistance = config.distance * 1.2

      scatterplotInstance.set({
        cameraDistance: defaultDistance,
        minCameraDistance: defaultDistance * (1 / 20),
        maxCameraDistance: defaultDistance * 3,
        cameraTarget: config.target,
      })

      config.scatterplot = scatterplotInstance
      config.regl = scatterplotInstance.get('regl')

    } catch (e) {
      if (regl) regl.destroy()
      throw e
    }
    resolve(config)
  });
}
