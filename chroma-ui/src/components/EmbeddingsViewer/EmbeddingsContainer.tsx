//@ts-nocheck
import React, { useState, useEffect, useRef } from 'react'
import scatterplot from './scatterplot'
import { Box, useTheme, useColorModeValue } from '@chakra-ui/react'

interface EmbeddingsContainerProps {
  points: any[][]
  toolSelected: string
  deselectHandler: () => void
  selectHandler: () => void
  unselectedPoints: []
  cursor: string
  colors: []
}

const EmbeddingsContainer: React.FC<EmbeddingsContainerProps> = ({ points, toolSelected, deselectHandler, selectHandler, unselectedPoints, cursor, colors }) => {
  let [reglInitialized, setReglInitialized] = useState(false);
  let [config, setConfig] = useState({})

  const theme = useTheme();
  const bgColor = useColorModeValue("#F3F5F6", '#0c0c0b')

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
    if (reglInitialized && points !== null) {
      config.scatterplot.draw(points)
    }
  }, [points])

  useEffect(() => {
    const resizeListener = () => {
      var canvas = document.getElementById("regl-canvas")
      var container = document.getElementById("regl-canvas-container")
      canvas.style.width = container?.clientWidth + "px"
      canvas.style.height = container?.clientHeight + "px"
    };
    window.addEventListener('resize', resizeListener);
    return () => {
      window.removeEventListener('resize', resizeListener);
    }
  }, [])

  function getRef(ref) {
    if (!ref) return;

    if (!reglInitialized && (points !== null)) {

      scatterplot(points,
        colors,
        {
          pixelRatio: Math.min(1.5, window.devicePixelRatio),
          canvas: ref,
          deselectHandler: deselectHandler,
          selectHandler: selectHandler
        }
      ).then(scatterplotConfig => {
        setReglInitialized(true)
        setConfig(scatterplotConfig)

      }).catch(err => {
        console.error("could not setup regl")
        setReglInitialized(false)
      });
    }
  }

  // how we set the cursor is a bit of a hack. if we have a custom cursor name
  // the cursor setting will fail, but our class will succeed in setting it
  // and vice versa
  return (
    <Box flex='1' cursor={cursor} className={cursor} id="regl-canvas-container" minWidth={0} marginTop="48px">
      <canvas
        id="regl-canvas"
        ref={getRef.bind(this)}
        style={{ backgroundColor: bgColor, height: "100%", width: "100%" }}
      ></canvas>
    </Box>
  )
}

export default React.memo(EmbeddingsContainer)
