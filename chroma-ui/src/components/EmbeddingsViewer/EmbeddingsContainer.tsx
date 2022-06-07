//@ts-nocheck
import React, { useState, useEffect, useRef } from 'react'
import scatterplot from './scatterplot'
import { Box, useTheme } from '@chakra-ui/react'

interface EmbeddingsContainerProps {
  points: any[][]
  toolSelected: string
  deselectHandler: () => void
  selectHandler: () => void
  unselectedPoints: []
  cursor: string
}

const EmbeddingsContainer: React.FC<EmbeddingsContainerProps> = ({ points, toolSelected, deselectHandler, selectHandler, unselectedPoints, cursor }) => {
  let [reglInitialized, setReglInitialized] = useState(false);
  let [config, setConfig] = useState({})

  const theme = useTheme();
  
  if (reglInitialized && (points !== null)) {
    if (toolSelected == 'lasso') {
      config.scatterplot.setLassoOverride(true)
    } else {
      config.scatterplot.setLassoOverride(false)
    }
    if (unselectedPoints.length !== 0) {
      config.scatterplot.deselectIds(unselectedPoints)
    }
  }

  useEffect(() => {
    if (reglInitialized && (points !== null)) {
      config.scatterplot.draw(points)
    }
  }, [points])

  // all the commented out code in this file has to do with resizing 
  // the webgl canvas... i havent figured it out yet

  // useEffect(() => {
  //   const resizeListener = () => {
  //     config.scatterplot.resizeHandler()
  //      var canvas = document.getElementById("regl-canvas")
  //     var container = canvas?.parentElement
  //     canvas.height = container?.offsetHeight
  //     canvas.width = container?.offsetWidth
  //   };
  //   window.addEventListener('resize', resizeListener);
  //   return () => {
  //     window.removeEventListener('resize', resizeListener);
  //   }
  // }, [config])

  // useEffect(() => {
  //   // timeoutId for debounce mechanism
  //   let timeoutId = null;
  //   const resizeListener = () => {
  //     // var canvas = document.getElementById("regl-canvas")
  //     // var container = canvas?.parentElement
  //     // canvas.height = container?.clientHeight
  //     // canvas.width = container?.clientWidth
  //     console.log('resize', config)
  //   };
  //   window.addEventListener('resize', resizeListener);
  //   return () => {
  //     window.removeEventListener('resize', resizeListener);
  //   }
  // }, [])
    
  function getRef (ref) {
    if (!ref) return;

    if (!reglInitialized && (points !== null)) {
      // const dimensions = getCanvasParentDimensions(ref)
      // ref.width = dimensions.w;
      // ref.height = dimensions.h;

      scatterplot(points, {
        pixelRatio: Math.min(1.5, window.devicePixelRatio),
        canvas: ref,
        deselectHandler: deselectHandler,
        selectHandler: selectHandler
      }).then(config => {
        setReglInitialized(true)
        setConfig(config)
        
      }).catch(err => {
        console.error("could not setup regl")
        setReglInitialized(false)
      });
    } 
  } 

  // function getCanvasParentDimensions(ref) {
  //   var parent = ref.parentNode,
  //       styles = getComputedStyle(parent),
  //       w = parseInt(styles.getPropertyValue("width"), 10),
  //       h = parseInt(styles.getPropertyValue("height"), 10);
  //   ref.width = w;
  //   ref.height = h;
  //   return {w: w, h:h}
  // }

  return (
    <Box flex='1' cursor={cursor}>
      <canvas 
        id="regl-canvas"
        ref={getRef.bind(this)} 
        style={{ backgroundColor: theme.colors.ch_gray.light, height: "100%", width: "100%" }}
      ></canvas>
    </Box>
  )
}

export default React.memo(EmbeddingsContainer)