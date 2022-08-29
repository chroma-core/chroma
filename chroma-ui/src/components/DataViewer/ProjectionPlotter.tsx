import React, { useState, useEffect } from 'react'
import scatterplot from './scatterplot'
import { Box, useColorModeValue, Center, Spinner, Select, Text } from '@chakra-ui/react'
import useResizeObserver from "use-resize-observer";
import { context__categoryFilterAtom, DataType, contextObjectSwitcherAtom, cursorAtom, context__datapointsAtom, context__datasetFilterAtom, globalDatapointAtom, globalProjectionsAtom, globalSelectedDatapointsAtom, globalVisibleDatapointsAtom, pointsToSelectAtom, context__projectionsAtom, selectedDatapointsAtom, context__tagFilterAtom, toolSelectedAtom, visibleDatapointsAtom, globalCategoryFilterAtom, globalCategoriesAtom, globalResourcesAtom, globalMetadataFilterAtom } from './atoms';
import { atom, useAtom } from 'jotai'
import { Projection, Datapoint, FilterArray, FilterType, Filter } from './types';
import { totalmem } from 'os';
import ImageRenderer from './ImageRenderer';
import { DataPanelGrid } from './DataPanel'

interface ConfigProps {
  scatterplot?: any
}

// we use this to figure out where to display the on-hover datapoint preview
const useMousePosition = () => {
  const [
    mousePosition,
    setMousePosition
  ] = React.useState({ x: null, y: null });
  React.useEffect(() => {
    const updateMousePosition = (ev: any) => {
      setMousePosition({ x: ev.clientX, y: ev.clientY });
    };
    window.addEventListener('mousemove', updateMousePosition);
    return () => {
      window.removeEventListener('mousemove', updateMousePosition);
    };
  }, []);
  return mousePosition;
};

// used to find the extents of the plotted points for automatic centering, setting zoom, and setting min/max zoom
const getBounds = (datapoints: { [key: number]: Datapoint }, projections: { [key: number]: Projection }) => {
  var minX = Infinity
  var minY = Infinity
  var maxX = -Infinity
  var maxY = -Infinity

  Object.values(datapoints).map(function (datapoint) {
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

// used with continuous filters and color interpolation
function minMaxNormalization(value: number, min: number, max: number) {
  return (value - min) / (max - min)
}

// this has to be out side of react because we need to keep its reference constant
function selectCallbackOutsideReact(points: any) {
  // @ts-ignore
  window.selectHandler(points)
}

interface PlotterProps {
  allFetched: boolean
}

const ProjectionPlotter: React.FC<PlotterProps> = ({ allFetched }) => {
  // bring in our atom data
  const [datapoints] = useAtom(globalDatapointAtom)
  const [categories] = useAtom(globalCategoriesAtom)
  const [resources] = useAtom(globalResourcesAtom)
  const [selectedDatapoints, updateselectedDatapoints] = useAtom(globalSelectedDatapointsAtom)
  const [visibleDatapoints] = useAtom(globalVisibleDatapointsAtom)
  const [projections] = useAtom(globalProjectionsAtom)
  const [metadataFilters, updateMetadataFilter] = useAtom(globalMetadataFilterAtom)

  // ui state atoms
  const [cursor] = useAtom(cursorAtom)
  const [toolSelected] = useAtom(toolSelectedAtom)
  const [pointsToSelect, setpointsToSelect] = useAtom(pointsToSelectAtom)
  const [contextObjectSwitcher, updatecontextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)

  // local state
  let [reglInitialized, setReglInitialized] = useState(false);
  let [boundsSet, setBoundsSet] = useState(false);
  let [config, setConfig] = useState<ConfigProps>({})
  let [points, setPoints] = useState<any>(undefined)
  let [datapointPointMap, setdatapointPointMap] = useState<{ [key: number]: number }>({})
  let [pointdatapointMap, setpointdatapointMap] = useState<{ [key: number]: number }>({})
  const [hoverPointId, setHoverPointId] = useState<number | undefined>(undefined)

  // hook
  const mousePosition = useMousePosition()

  const updateMetadata = (data: any, fn: any) => {
    // this is a bit of a hack
    updateMetadataFilter({ ...metadataFilters })
  }

  // fetch and prep metadata filters for color by
  var metatadataFilterMap = Object.values(metadataFilters).map(m => {
    return { filter: m, update: updateMetadata, name: m.name }
  })

  // my own custom enum class so i can add to it at runtime
  let totalColorByOptions = 2;
  let ColorByOptionsArr: { [key: string | number]: string | number; } = {
    0: 'None',
    1: 'Categories',
    'None': 0,
    'Categories': 1,
  }

  // local state for which color by option we currently have selected and what the color options are for it
  let [colorByFilterEnum, setColorByFilterEnum] = useState(ColorByOptionsArr.None)
  let [colorByOptions, setColorByOptions] = useState(['#fe115d', '#65c00c', '#6641de', '#fa6d09', '#015be8', '#d84500', '#3b21b3', '#e90042', '#8e63f8', '#f338c2'])

  // this is a dummy filter we create here to let the user color by None (all gray)
  let noneFilter: Filter = {
    name: 'None',
    type: FilterType.Discrete,
    //@ts-ignore
    options: [{ color: "#111", id: 0, visible: true, evalDatapoint: () => { } }],
    linkedAtom: [],
    fetchFn: (datapoint) => {
      return datapoint.annotations[0].category_id
    }
  }

  const [categoryFilter] = useAtom(context__categoryFilterAtom)
  const filterArray: any[] = []
  if (contextObjectSwitcher == DataType.Object) {
    filterArray.push(
      { name: ColorByOptionsArr.None, filter: noneFilter },
      { name: ColorByOptionsArr.Categories, filter: categoryFilter! },
      ...metatadataFilterMap
    )
    metatadataFilterMap.forEach(mF => {
      totalColorByOptions++
      ColorByOptionsArr[mF.filter.name] = mF.filter.name
      ColorByOptionsArr[totalColorByOptions] = mF.filter.name
    })
  }
  if (contextObjectSwitcher == DataType.Context) {
    filterArray.push(
      { name: ColorByOptionsArr.None, filter: noneFilter },
      ...metatadataFilterMap
    )
    metatadataFilterMap.forEach(mF => {
      totalColorByOptions++
      ColorByOptionsArr[mF.filter.name] = mF.filter.name
      ColorByOptionsArr[totalColorByOptions] = mF.filter.name
    })
  }

  // whenever colorByFilterString change, redraw
  useEffect(() => {
    if (!allFetched) return
    calculateColorsAndDrawPoints()
  }, [colorByFilterEnum])

  let [target, setTarget] = useState<any>(undefined)
  let [maxSize, setMaxSize] = useState<any>(undefined)

  const bgColor = useColorModeValue("#F3F5F6", '#0c0c0b')
  const { ref, width = 1, height = 1 } = useResizeObserver<HTMLDivElement>({
    onResize: ({ width, height }) => { // eslint-disable-line @typescript-eslint/no-shadow
      if (config.scatterplot !== undefined) {
        config.scatterplot.resizeHandler()
        resizeListener()
      }
    }
  })

  useEffect(() => {
    if (Object.values(datapoints).length == 0) return
    if (Object.values(projections).length == 0) return
    if (!allFetched) return
    if (config.scatterplot == undefined) return
    if (boundsSet == false) {
      let bounds = getBounds(datapoints, projections)
      config.scatterplot.set({
        cameraDistance: (bounds.maxSize * 1.4) * 3,
        minCameraDistance: (bounds.maxSize * 1.4) * (1 / 20),
        maxCameraDistance: (bounds.maxSize * 1.4) * 8,
        cameraTarget: [bounds.centerX, bounds.centerY],
      })
      setBoundsSet(true)
    }
  }, [config, boundsSet])

  // Callback functions that are fired by regl-scatterplot
  // @ts-ignore
  const selectHandler = ({ points: newSelectedPoints }) => {
    const t3 = performance.now();
    if (pointsToSelect.length > 0) return
    var sdp: number[] = []
    newSelectedPoints.map((pointId: any) => {
      sdp.push(pointdatapointMap[pointId])
    })
    updateselectedDatapoints(sdp)
    const t4 = performance.now();
    // console.log(`selectHandler hook: ${(t4 - t3) / 1000} seconds.`);
  }
  // @ts-ignore
  window.selectHandler = selectHandler; // eslint-disable-line @typescript-eslint/no-this-alias
  const deselectHandler = () => {
    updateselectedDatapoints([])
    setpointsToSelect([])
  };
  const pointOverHandler = (pointId: number) => {
    setHoverPointId(pointId)
  };
  const pointOutHandler = () => {
    setHoverPointId(undefined)
  };

  // whenever datapoints changes, we want to regenerate out points and send them down to plotter
  // 1.5s across 70k datapoints, running 2 times! every time a new batch of data is loaded in
  useEffect(() => {
    if (!allFetched) return
    const t3 = performance.now();
    if (Object.values(datapoints).length == 0) return
    if (Object.values(projections).length == 0) {
      setPoints([])
      return
    }
    let bounds = getBounds(datapoints, projections)
    setTarget([bounds.centerX, bounds.centerY])
    setMaxSize(bounds.maxSize)
    calculateColorsAndDrawPoints()
    setBoundsSet(true)
    const t4 = performance.now();
    // console.log(`datapoints hook: ${(t4 - t3) / 1000} seconds.`);
  }, [datapoints, contextObjectSwitcher])

  // fired when we switch between object and context
  useEffect(() => {
    if (reglInitialized && (points !== null) && (config.scatterplot !== undefined)) {
      // right now we wipe selection when you switch modes
      config.scatterplot.select([])
      setBoundsSet(false)
    }
  }, [contextObjectSwitcher])

  // whenever datapoints changes, we want to regenerate out points and send them down to plotter
  // 1.5s across 70k datapoints, running 3 times! every time a new batch of data is loaded in
  useEffect(() => {
    if (!allFetched) return
    const t3 = performance.now();
    if (Object.values(datapoints).length == 0) return
    if (Object.values(projections).length == 0) return
    calculateColorsAndDrawPoints()
    const t4 = performance.now();
    // console.log(`datapoints, visibleDatapoints hook: ${(t4 - t3) / 1000} seconds.`);
  }, [visibleDatapoints])

  useEffect(() => {
    const t3 = performance.now();
    if (pointsToSelect.length === 0) return
    if (reglInitialized && (points !== null) && (config.scatterplot !== undefined)) {

      let selectionPoints: number[] = []
      pointsToSelect.map(dpid => {
        selectionPoints.push(datapointPointMap[dpid])
      })

      config.scatterplot.select(selectionPoints)
      setpointsToSelect([])
    }
    const t4 = performance.now();
    // console.log(`pointsToSelect hook: ${(t4 - t3) / 1000} seconds.`);
  }, [pointsToSelect])

  if (reglInitialized && (points !== null)) {
    if (toolSelected == 'lasso') {
      config.scatterplot.setLassoOverride(true)
    } else {
      config.scatterplot.setLassoOverride(false)
    }
  }

  // whenever points change, redraw
  // fast, 0.0001s fast
  useEffect(() => {
    if (reglInitialized && points !== null) {
      config.scatterplot.set({ pointColor: colorByOptions });
      config.scatterplot.draw(points)
    }
  }, [points])

  // 95% of the time cost of the datapoints hookS is in this fn
  const calculateColorsAndDrawPoints = () => {
    const t3 = performance.now();
    let colorByFilter = filterArray.find((a: any) => a.name == ColorByOptionsArr[colorByFilterEnum])
    console.log('colorByFilter', colorByFilter)

    let colorByOptionsSave
    if (colorByFilter?.filter.type == FilterType.Discrete) colorByOptionsSave = colorByFilter.filter.options!.map((option: any) => option.color)
    if (colorByFilter?.filter.type == FilterType.Continuous) colorByOptionsSave = colorByFilter.filter.range!.colorScale
    setColorByOptions(colorByOptionsSave) // sets the array of colors that the plotter should use

    let datapointsClone = Object.assign({}, datapoints)
    Object.values(datapointsClone).map(function (datapoint) {
      datapoint.visible = false
    })
    visibleDatapoints.forEach(vdp => datapointsClone[vdp].visible = true)

    let datapointPointMapObject: { [key: number]: number } = {}
    let pointdatapointObject: { [key: number]: number } = {}

    points = [[0, 0, 0, 0]] // this make the ids in regl-scatterplot (zero-indexed) match our database ids (not zero-indexed)
    var i = 0
    Object.values(datapointsClone).map(function (datapoint) {
      datapointPointMapObject[datapoint.id] = points.length //+ 1
      pointdatapointObject[points.length] = datapoint.id

      // get the category id/name, whatever is relevant from the datapoint
      let datapointColorByProp = colorByFilter?.filter.fetchFn(datapoint)
      // if (i < 10) {
      //   console.log('datapointColorByProp', datapointColorByProp)
      //   i++
      // }

      // then lookup in that filter what the color should be, and its position in the list
      let datapointColorIndex = 0
      if (colorByFilter?.filter.type == FilterType.Discrete) datapointColorIndex = colorByFilter?.filter.options!.findIndex((option: any) => option.id == datapointColorByProp)
      if (colorByFilter?.filter.type == FilterType.Continuous) datapointColorIndex = minMaxNormalization(datapointColorByProp, colorByFilter?.filter.range!.min, colorByFilter?.filter.range!.max) // normalize

      return points.push([projections[datapoint.projection_id].x, projections[datapoint.projection_id].y, datapoint.visible, datapointColorIndex, datapoint.id])
    })
    setdatapointPointMap(datapointPointMapObject)
    setpointdatapointMap(pointdatapointObject)
    if (points.length > 1) setPoints(points)
    const t4 = performance.now();
    // console.log(`calculateColorsAndDrawPoints: ${(t4 - t3) / 1000} seconds.`);
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
    if (!canvasRef) return
    if (!boundsSet) return
    if (!reglInitialized && (points !== null)) {
      scatterplot(points,
        colorByOptions,
        {
          pixelRatio: Math.min(1.5, window.devicePixelRatio),
          canvas: canvasRef,
          deselectHandler: deselectHandler,
          selectHandler: selectCallbackOutsideReact,
          pointOverHandler: pointOverHandler,
          pointOutHandler: pointOutHandler,
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
    setColorByFilterEnum(event.target.value)
  }

  let showLoading = false
  if (Object.values(datapoints).length === 0) showLoading = true

  const hoverDatapointObject = (hoverPointId !== undefined) ? datapoints[pointdatapointMap[hoverPointId]] : undefined
  const uri = (hoverDatapointObject !== undefined) ? resources[hoverDatapointObject.resource_id].uri : undefined

  // how we set the cursor is a bit of a hack. if we have a custom cursor name
  // the cursor setting will fail, but our class will succeed in setting it
  // and vice versa
  return (
    <Box flex='1' ref={ref} cursor={cursor} className={cursor} id="regl-canvas-container" minWidth={0} marginTop="48px" width="800px">
      {(filterArray.length > 0) ?
        <Select pos="absolute" width={150} marginTop="10px" marginLeft="10px" value={colorByFilterEnum} onChange={newColorBy}>
          {filterArray.map((key) => {
            return (
              <option key={ColorByOptionsArr[key.name]} value={ColorByOptionsArr[key.name]} >{ColorByOptionsArr[key.name]}</option>
            )
          })}
        </Select>
        : null}
      {(hoverPointId !== undefined) ?
        <Box zIndex={999} width="150px" height="150px" pos="absolute" top={(mousePosition.y !== null) ? mousePosition.y + 10 : 0} left={(mousePosition.x !== null) ? mousePosition.x + 10 : 0}>
          <DataPanelGrid datapoint={datapoints[pointdatapointMap[hoverPointId]]} index={0} />
        </Box>
        : null}
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