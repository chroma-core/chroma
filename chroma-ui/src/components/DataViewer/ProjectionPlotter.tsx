import React, { useState, useEffect } from 'react'
import scatterplot from './scatterplot'
import { Box, useColorModeValue, Center, Spinner, Select, Text } from '@chakra-ui/react'
import useResizeObserver from "use-resize-observer";
// import { context__categoryFilterAtom, DataType, contextObjectSwitcherAtom, cursorAtom, globalDatapointAtom, globalProjectionsAtom, globalSelectedDatapointsAtom, globalVisibleDatapointsAtom, pointsToSelectAtom, toolSelectedAtom, globalCategoriesAtom, globalResourcesAtom, globalMetadataFilterAtom, globalDatasetFilterAtom, object__datapointsAtom } from './atoms';
import { object__inferenceCategoryFilterAtom, context__categoryFilterAtom, DataType, contextObjectSwitcherAtom, cursorAtom, globalDatasetFilterAtom, context__datapointsAtom, context__datasetFilterAtom, globalDatapointAtom, globalProjectionsAtom, globalSelectedDatapointsAtom, globalVisibleDatapointsAtom, pointsToSelectAtom, context__projectionsAtom, selectedDatapointsAtom, context__tagFilterAtom, toolSelectedAtom, visibleDatapointsAtom, globalCategoryFilterAtom, globalCategoriesAtom, globalResourcesAtom, globalMetadataFilterAtom, object__datapointsAtom, globaldatapointToPointMapAtom, globalplotterBoundsAtom, hoverToHighlightInPlotterDatapointIdAtom, object__categoryFilterAtom } from './atoms';
import { atom, useAtom } from 'jotai'
import { Projection, Datapoint, FilterType, Filter } from './types';
import { totalmem } from 'os';
import ImageRenderer from './ImageRenderer';
import { DataPanelGrid } from './DataPanel'
import { ConfigProps, getBounds, minMaxNormalization, PlotterProps, selectCallbackOutsideReact, useMousePosition, viewCallbackOutsideReact } from './PlotterUtils';


const ProjectionPlotter: React.FC<PlotterProps> = ({ allFetched }) => {
  // bring in our atom data
  const [datapoints] = useAtom(globalDatapointAtom)
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
  const [hoverPoint, setHoverPoint] = useAtom(hoverToHighlightInPlotterDatapointIdAtom)

  const [hoverPointId, setHoverPointId] = useState<number | undefined>(undefined)
  let [points, setPoints] = useState<any>(undefined)
  let [target, setTarget] = useState<any>(undefined)
  let [maxSize, setMaxSize] = useState<any>(undefined)

  const [plotterBounds, setPlotterBounds] = useAtom(globalplotterBoundsAtom)

  const bgColor = useColorModeValue("#F3F5F6", '#0c0c0b')

  // local state
  let [reglInitialized, setReglInitialized] = useState(false);
  let [config, setConfig] = useState<ConfigProps>({})
  let [datapointPointMap, setdatapointPointMap] = useAtom(globaldatapointToPointMapAtom)//useState<{ [key: number]: number }>({})
  let [pointdatapointMap, setpointdatapointMap] = useState<{ [key: number]: number }>({})

  const [object__datapoints] = useAtom(object__datapointsAtom)

  // hook
  const mousePosition = useMousePosition()

  let showLoading = false
  if (Object.values(datapoints).length === 0) showLoading = true

  if (hoverPoint !== undefined) {
    config.scatterplot.forceHover(datapointPointMap[hoverPoint])
  }

  // 
  // Related to our color by dropdown
  // 

  const updateMetadata = (data: any, fn: any) => {
    // this is a bit of a hack
    updateMetadataFilter({ ...metadataFilters })
  }

  // fetch and prep metadata filters for color by
  var metatadataFilterMap = Object.values(metadataFilters).map(m => {
    return { filter: m, update: updateMetadata, name: m.name }
  })

  // my own custom enum class so i can add to it at runtime
  let totalColorByOptions = 3;
  let ColorByOptionsArr: { [key: string | number]: string | number; } = {
    0: 'None',
    1: 'LabelCategories',
    2: 'Datasets',
    3: 'InferenceCategories',
    'None': 0,
    'LabelCategories': 1,
    'Datasets': 2,
    'InferenceCategories': 3,
  }

  // local state for which color by option we currently have selected and what the color options are for it
  let [colorByFilterEnum, setColorByFilterEnum] = useState(ColorByOptionsArr.None)
  let [colorByOptions, setColorByOptions] = useState(['#111'])

  // this is a dummy filter we create here to let the user color by None (all gray)
  let noneFilter: Filter = {
    name: 'None',
    type: FilterType.Discrete,
    //@ts-ignore
    options: [{ color: "#111", id: 0, visible: true, evalDatapoint: () => { } }],
    linkedAtom: [],
    fetchFn: (datapoint) => {
      return ""//datapoint.annotations[0].category_id
    }
  }

  const [categoryFilter] = useAtom(object__inferenceCategoryFilterAtom)
  const [categoryFilter2] = useAtom(object__categoryFilterAtom)
  const [datasetFilter] = useAtom(globalDatasetFilterAtom)
  const filterArray: any[] = []
  if (contextObjectSwitcher == DataType.Object) {
    filterArray.push(
      { name: ColorByOptionsArr.None, filter: noneFilter },
      { name: ColorByOptionsArr.LabelCategories, filter: categoryFilter2! },
      { name: ColorByOptionsArr.InferenceCategories, filter: categoryFilter! },
      { name: ColorByOptionsArr.Datasets, filter: datasetFilter! },
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
      { name: ColorByOptionsArr.Datasets, filter: datasetFilter! },
      ...metatadataFilterMap
    )
    if (Object.values(object__datapoints).length === 0) {
      filterArray.push({ name: ColorByOptionsArr.LabelCategories, filter: categoryFilter! })
    }
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

  const newColorBy = (event: any) => {
    setColorByFilterEnum(event.target.value)
  }


  // 
  // callbacks
  // 

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
  }
  const pointOverHandler = (pointId: number) => {
    setHoverPointId(pointId)
  }
  const pointOutHandler = () => {
    setHoverPointId(undefined)
  }
  // this is running more often than i want..... even just a mouse move triggers it
  // i really only want the event where the camera updates
  const viewChangedHandler = (viewData: any) => {
    setHoverPoint(undefined)
    if (plotterBounds.cameraDistance === undefined) return
    let plotterBoundsToUpdate = Object.assign({}, plotterBounds)
    plotterBoundsToUpdate.cameraDistance = viewData.camera.distance[0]
    plotterBoundsToUpdate.cameraTarget = [viewData.camera.target[0], viewData.camera.target[1]]
    setPlotterBounds(plotterBoundsToUpdate)
  }
  // @ts-ignore
  window.viewHandler = viewChangedHandler; // eslint-disable-line @typescript-eslint/no-this-alias

  // 
  // Drawing stuff
  // 

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

    let localPlotterBounds
    if (plotterBounds.cameraDistance !== undefined) {
      localPlotterBounds = plotterBounds
    } else {
      let bounds = getBounds(datapoints, projections)
      localPlotterBounds = {
        cameraDistance: (bounds.maxSize * 1.4) * 1,
        minCameraDistance: (bounds.maxSize * 1.4) * (1 / 20),
        maxCameraDistance: (bounds.maxSize * 1.4) * 8,
        cameraTarget: [bounds.centerX, bounds.centerY],
        maxSize: bounds.maxSize
      }
      setPlotterBounds(localPlotterBounds)
    }
    config.scatterplot.set(localPlotterBounds)
    setTarget([localPlotterBounds.cameraTarget![0], localPlotterBounds.cameraTarget![1]])
    setMaxSize(localPlotterBounds.maxSize)
    calculateColorsAndDrawPoints()
    // const t4 = performance.now();
    // console.log(`datapoints hook: ${(t4 - t3) / 1000} seconds.`);
  }, [datapoints])

  // whenever datapoints changes, we want to regenerate out points and send them down to plotter
  useEffect(() => {
    if (!allFetched) return
    const t3 = performance.now();
    if (Object.values(datapoints).length == 0) return
    if (Object.values(projections).length == 0) return
    calculateColorsAndDrawPoints()
    const t4 = performance.now();
  }, [visibleDatapoints])

  // when we select points from elsewhere in the application
  // this triggers them being selected in the plotter
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

  // whenever points change, redraw
  useEffect(() => {
    if (reglInitialized && points !== null) {
      config.scatterplot.set({ pointColor: colorByOptions });
      config.scatterplot.draw(points)

      // i have to wait until the redraw happens... this is a dumb hack
      window.requestAnimationFrame(() => {
        let selectionPoints: number[] = []
        selectedDatapoints.map(dpid => {
          selectionPoints.push(datapointPointMap[dpid])
        })
        config.scatterplot.select(selectionPoints)
      })

    }
  }, [points])

  // this converts datapoints into points, including their color and whether or not they are visible
  const calculateColorsAndDrawPoints = () => {
    const t3 = performance.now();
    let colorByFilter = filterArray.find((a: any) => a.name == ColorByOptionsArr[colorByFilterEnum])

    let colorByOptionsSave
    if (colorByFilter?.filter.type == FilterType.Discrete) colorByOptionsSave = colorByFilter.filter.options!.map((option: any) => option.color)
    if (colorByFilter?.filter.type == FilterType.Continuous) colorByOptionsSave = colorByFilter.filter.range!.colorScale
    setColorByOptions(colorByOptionsSave) // sets the array of colors that the plotter should use

    let datapointsClone = Object.assign({}, datapoints)
    Object.values(datapointsClone).map(function (datapoint) {
      datapoint.visible = false // reset them all to hidden
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

      // then lookup in that filter what the color should be, and its position in the list
      let datapointColorIndex = 0
      if (colorByFilter?.filter.type == FilterType.Discrete) datapointColorIndex = colorByFilter?.filter.options!.findIndex((option: any) => option.id == datapointColorByProp)
      if (colorByFilter?.filter.type == FilterType.Continuous) datapointColorIndex = minMaxNormalization(datapointColorByProp, colorByFilter?.filter.range!.min, colorByFilter?.filter.range!.max) // normalize

      return points.push([projections[datapoint.projection_id].x, projections[datapoint.projection_id].y, datapoint.visible, datapointColorIndex, datapoint.id])
    })

    // useful for going from point to datapoint and datapoint to point quickly
    setdatapointPointMap(datapointPointMapObject)
    setpointdatapointMap(pointdatapointObject)

    if (points.length > 1) setPoints(points)
    const t4 = performance.now();
    // console.log(`calculateColorsAndDrawPoints: ${(t4 - t3) / 1000} seconds.`);
  }

  if (reglInitialized && (points !== null)) {
    if (toolSelected == 'lasso') {
      config.scatterplot.setLassoOverride(true)
    } else {
      config.scatterplot.setLassoOverride(false)
    }
  }

  // 
  // Resize stuff
  // 

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

  const { ref, width = 1, height = 1 } = useResizeObserver<HTMLDivElement>({
    onResize: ({ width, height }) => { // eslint-disable-line @typescript-eslint/no-shadow
      if (config.scatterplot !== undefined) {
        config.scatterplot.resizeHandler()
        resizeListener()
      }
    }
  })

  function getRef(canvasRef: any) {
    if (!canvasRef) return
    // if (!boundsSet) return
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
          viewChangedHandler: viewCallbackOutsideReact,
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