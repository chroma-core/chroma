import React, { useState, useEffect } from 'react'
import scatterplot from './scatterplot'
import { Box, useColorModeValue, Center, Spinner, Select } from '@chakra-ui/react'
import useResizeObserver from "use-resize-observer";
import { categoryFilterAtom, cursorAtom, datapointsAtom, datasetFilterAtom, pointsToSelectAtom, projectionsAtom, selectedDatapointsAtom, tagFilterAtom, toolSelectedAtom, visibleDatapointsAtom } from './atoms';
import { atom, useAtom } from 'jotai'
import { Projection, Datapoint, FilterArray, FilterType } from './types';

interface ConfigProps {
  scatterplot?: any
}

const readDatapointsAtom = atom((get) => get(datapointsAtom))

const getBounds = (datapoints: { [key: number]: Datapoint }, projections: { [key: number]: Projection }) => {
  var minX = Infinity
  var minY = Infinity
  var maxX = -Infinity
  var maxY = -Infinity

  Object.keys(datapoints).map(function (keyName, keyIndex) {
    let datapoint = datapoints[parseInt(keyName, 10)]
    if (projections[datapoint.projection_id!].y < minY) minY = projections[datapoint.projection_id!].y
    if (projections[datapoint.projection_id!].y > maxY) maxY = projections[datapoint.projection_id!].y
    if (projections[datapoint.projection_id!].x < minX) minX = projections[datapoint.projection_id!].x
    if (projections[datapoint.projection_id!].x > maxX) maxX = projections[datapoint.projection_id!].x
  })

  var centerX = (maxX + minX) / 2
  var centerY = (maxY + minY) / 2

  var sizeX = (maxX - minX) / 2
  var sizeY = (maxY - minY) / 2

  return {
    minX: minX,
    maxX: maxX,
    minY: minY,
    maxY: maxY,
    centerX: centerX,
    centerY: centerY,
    maxSize: (sizeX > sizeY) ? sizeX : sizeY
  }
}

function minMaxNormalization(value: number, min: number, max: number) {
  return (value - min) / (max - min)
}

// @ts-ignore
// let window.currentInstance: any = null
function selectCallbackOutsideReact(points: any) {
  // @ts-ignore
  window.selectHandler(points)
}

const ProjectionPlotter: React.FC = () => {
  const [datapoints] = useAtom(datapointsAtom)
  const [readDatapoints] = useAtom(readDatapointsAtom)
  const [selectedDatapoints, updateselectedDatapoints] = useAtom(selectedDatapointsAtom)
  const [visibleDatapoints] = useAtom(visibleDatapointsAtom)
  const [projections] = useAtom(projectionsAtom)
  const [cursor] = useAtom(cursorAtom)
  const [toolSelected] = useAtom(toolSelectedAtom)
  const [pointsToSelect, setpointsToSelect] = useAtom(pointsToSelectAtom)

  let [reglInitialized, setReglInitialized] = useState(false);
  let [boundsSet, setBoundsSet] = useState(false);
  let [config, setConfig] = useState<ConfigProps>({})
  let [points, setPoints] = useState<any>(undefined)
  let [target, setTarget] = useState<any>(undefined)
  let [maxSize, setMaxSize] = useState<any>(undefined)
  let [colorByFilterString, setColorByFilterString] = useState('Inferences')
  let [colorByOptions, setColorByOptions] = useState(['#fe115d', '#65c00c', '#6641de', '#fa6d09', '#015be8', '#d84500', '#3b21b3', '#e90042', '#8e63f8', '#f338c2'])
  const bgColor = useColorModeValue("#F3F5F6", '#0c0c0b')
  const { ref, width = 1, height = 1 } = useResizeObserver<HTMLDivElement>({
    onResize: ({ width, height }) => { // eslint-disable-line @typescript-eslint/no-shadow
      if (config.scatterplot !== undefined) {
        config.scatterplot.resizeHandler()
        resizeListener()
      }
    }
  })

  // Callback functions that are fired by regl-scatterplot
  // @ts-ignore
  const selectHandler = ({ points: newSelectedPoints }) => {
    if (pointsToSelect.length > 0) return
    const selectedPoints = newSelectedPoints.map((id: number) => {
      var datapointId = Object.keys(datapoints)[id - 1]
      return parseInt(datapointId, 10)
    })
    if (selectedPoints == selectedDatapoints) return
    updateselectedDatapoints(selectedPoints)
  }

  // @ts-ignore
  window.selectHandler = selectHandler; // eslint-disable-line @typescript-eslint/no-this-alias

  const deselectHandler = () => {
    updateselectedDatapoints([])
    setpointsToSelect([])
  };

  const [categoryFilter, updatecategoryFilter] = useAtom(categoryFilterAtom)
  const [tagFilter, updatetagFilter] = useAtom(tagFilterAtom)
  const [datasetFilter, updatedatasetFilter] = useAtom(datasetFilterAtom)

  const filterArray: FilterArray[] = [
    { filter: categoryFilter!, update: updatecategoryFilter },
    { filter: tagFilter!, update: updatetagFilter },
    { filter: datasetFilter!, update: updatedatasetFilter }
  ]

  // whenever datapoints changes, we want to regenerate out points and send them down to plotter
  useEffect(() => {
    if (Object.keys(datapoints).length == 0) return

    let bounds = getBounds(datapoints, projections)
    setTarget([bounds.centerX, bounds.centerY])
    setMaxSize(bounds.maxSize)
    calculateColorsAndDrawPoints()
  }, [datapoints])

  useEffect(() => {
    if (boundsSet) return
    if (config.scatterplot == undefined) return
    if (boundsSet == false) {
      let bounds = getBounds(datapoints, projections)
      config.scatterplot.set({
        cameraDistance: (bounds.maxSize * 1.4),
        minCameraDistance: (bounds.maxSize * 1.4) * (1 / 20),
        maxCameraDistance: (bounds.maxSize * 1.4) * 3,
        cameraTarget: [bounds.centerX, bounds.centerY],
      })
      setBoundsSet(true)
    }
  }, [config])

  // whenever datapoints changes, we want to regenerate out points and send them down to plotter
  useEffect(() => {
    if (datapoints === undefined) return
    calculateColorsAndDrawPoints()
  }, [datapoints, visibleDatapoints])

  useEffect(() => {
    if (pointsToSelect.length === 0) return
    if (reglInitialized && (points !== null) && (config.scatterplot !== undefined)) {
      // go from Ids to indices... 
      var select: number[] = []
      pointsToSelect.map(p => {
        select.push(Object.keys(datapoints).findIndex(i => parseInt(i, 10) == p) + 1)
      })

      config.scatterplot.select(select)
      setpointsToSelect([])
    }
  }, [pointsToSelect])

  if (reglInitialized && (points !== null)) {
    if (toolSelected == 'lasso') {
      config.scatterplot.setLassoOverride(true)
    } else {
      config.scatterplot.setLassoOverride(false)
    }
  }

  // whenever points change, redraw
  useEffect(() => {
    if (reglInitialized && points !== null) {
      config.scatterplot.set({ pointColor: colorByOptions });
      config.scatterplot.draw(points)
    }
  }, [points])

  // whenever colorByFilterString change, redraw
  // useEffect(() => {
  //   if (insertedProjections !== true) return
  //   if (datapoints === undefined) return
  //   calculateColorsAndDrawPoints()
  // }, [colorByFilterString])

  const calculateColorsAndDrawPoints = () => {
    // let colorByFilter = filterArray.find((a: any) => a.filter.name == colorByFilterString)

    // let colorByOptionsSave
    // if (colorByFilter?.filter.type == FilterType.Discrete) colorByOptionsSave = colorByFilter.filter.options!.map((option: any) => option.color)
    // if (colorByFilter?.filter.type == FilterType.Continuous) colorByOptionsSave = colorByFilter.filter.range!.colorScale
    // // @ts-ignore
    // setColorByOptions(colorByOptionsSave)

    points = [[0, 0, 0, 0]] // this make the ids in regl-scatterplot (zero-indexed) match our database ids (not zero-indexed)
    Object.keys(datapoints).map(function (keyName, keyIndex) {
      let datapoint = datapoints[parseInt(keyName, 10)]
      // let datapointColorByProp = colorByFilter?.filter.fetchFn(datapoint)[0]

      // let datapointColorIndex
      // if (colorByFilter?.filter.type == FilterType.Discrete) datapointColorIndex = colorByFilter?.filter.options!.findIndex((option: any) => option.name == datapointColorByProp)
      // if (colorByFilter?.filter.type == FilterType.Continuous) datapointColorIndex = minMaxNormalization(datapointColorByProp, colorByFilter?.filter.range!.min, colorByFilter?.filter.range!.max) // normalize

      const visible = visibleDatapoints.includes(datapoint.id) ? 1 : 0
      return points.push([projections[datapoint.projection_id].x, projections[datapoint.projection_id].y, visible, 0, parseInt(keyName, 10)])
    })
    if (points.length > 1) setPoints(points)
  }

  const resizeListener = () => {
    var canvas = document.getElementById("regl-canvas")
    var container = document.getElementById("regl-canvas-container")
    if (canvas !== null) {
      canvas.style.width = container?.clientWidth + "px"
      canvas.style.height = container?.clientHeight + "px"
    }
  };

  // resize our scatterplot on window resize
  useEffect(() => {
    window.addEventListener('resize', resizeListener);
    return () => {
      window.removeEventListener('resize', resizeListener);
    }
  }, [])

  function getRef(canvasRef: any) {
    if (!canvasRef) return;
    if (!reglInitialized && (points !== null)) {
      scatterplot(points,
        colorByOptions,
        {
          pixelRatio: Math.min(1.5, window.devicePixelRatio),
          canvas: canvasRef,
          deselectHandler: deselectHandler,
          selectHandler: selectCallbackOutsideReact,
          target: target,
          distance: maxSize * 1.2
        }
      ).then((scatterplotConfig: any) => {
        setReglInitialized(true)
        setConfig(scatterplotConfig)
      }).catch(err => {
        console.error("could not setup regl")
        setReglInitialized(false)
      });
    }
  }

  const newColorBy = (event: any) => {
    setColorByFilterString(event.target.value)
  }

  let showLoading = false
  if (Object.keys(datapoints).length === 0) showLoading = true

  // let validFilters
  // if (filters !== undefined) {
  //   const noFilterList = ["Tags"]
  //   validFilters = filters.filter((f: any) => !noFilterList.includes(f.name))
  // }

  // how we set the cursor is a bit of a hack. if we have a custom cursor name
  // the cursor setting will fail, but our class will succeed in setting it
  // and vice versa
  return (
    <Box flex='1' ref={ref} cursor={cursor} className={cursor} id="regl-canvas-container" minWidth={0} marginTop="48px" width="800px">
      {/* {(filters !== undefined) ?
        <Select pos="absolute" width={150} marginTop="10px" marginLeft="10px" value={colorByFilterString} onChange={newColorBy}>
          {validFilters.map((filterb: any) => {
            return (
              <option key={filterb.name} value={filterb.name} >{filterb.name}</option>
            )
          })}
        </Select>
        : null} */}
      {
        showLoading ?
          <Center height="100vh" bgColor={bgColor} >
            <Spinner size='xl' />
          </Center >
          :
          <canvas
            id="regl-canvas"
            ref={getRef.bind(this)}
            style={{ backgroundColor: bgColor, height: "100%", width: "100%" }}
          ></canvas>
      }
    </Box>
  )
}

export default ProjectionPlotter