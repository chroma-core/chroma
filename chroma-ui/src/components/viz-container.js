import React, { useState, useEffect } from 'react'
import triangleViz from './triangle-viz'
import scatterplot from './scatterplot'

export default function TestViz({data}) {
  let [reglRef, setReglRef] = useState(null);
  let [reglInitialized, setReglInitialized] = useState(false);
  
  function getRef (ref) {
    if (!ref) return;

    if (!reglInitialized) {
      scatterplot({
        pixelRatio: Math.min(1.5, window.devicePixelRatio),
        canvas: ref,
      }).then(config => {
        setReglInitialized(true)
        setReglRef(config)
      }).catch(err => {
        console.error("could not setup regl")
        setReglInitialized(false)
      });
    }
  } 

  // on unload
  useEffect(() => {
    return () => {
      if (reglRef) {
        reglRef.regl.destroy()
      }
    };
  }, []);

  return (
    <div>
      <p>things go here</p>
      <canvas width={500} height={500} ref={getRef.bind(this)}></canvas>
    </div>
  )
}
