import React, { useState, useEffect, useRef } from 'react'
import scatterplot from './scatterplot'
import { Box, useTheme, useColorModeValue, Center, Spinner, Select } from '@chakra-ui/react'
import { Datapoint } from './DataViewTypes'
import { filter } from 'lodash'

interface ProjectionPlotterProps {
    datapoints?: Datapoint[]
    toolSelected: string
    showLoading: boolean
    filters?: any
    insertedProjections: boolean
    deselectHandler: () => void
    selectHandler: () => void
    cursor: string
}

interface ConfigProps {
    scatterplot?: any
}

const getBounds = (datapoints: Datapoint[]) => {
    var minX = Infinity
    var minY = Infinity
    var maxX = -Infinity
    var maxY = -Infinity

    datapoints.forEach((datapoint) => {
        if (datapoint.projection!.y < minY) minY = datapoint.projection!.y
        if (datapoint.projection!.y > maxY) maxY = datapoint.projection!.y
        if (datapoint.projection!.x < minX) minX = datapoint.projection!.x
        if (datapoint.projection!.x > maxX) maxX = datapoint.projection!.x
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

const ProjectionPlotter: React.FC<ProjectionPlotterProps> = ({ cursor, insertedProjections, datapoints, showLoading, toolSelected, filters, selectHandler, deselectHandler }) => {
    let [reglInitialized, setReglInitialized] = useState(false);
    let [boundsSet, setBoundsSet] = useState(false);
    let [config, setConfig] = useState<ConfigProps>({})
    let [points, setPoints] = useState<any>(undefined)
    let [target, setTarget] = useState<any>(undefined)
    let [maxSize, setMaxSize] = useState<any>(undefined)
    let [colorByFilterString, setColorByFilterString] = useState('Classes')
    let [colorByOptions, setColorByOptions] = useState([])
    const bgColor = useColorModeValue("#F3F5F6", '#0c0c0b')

    // let cursor = 'pointer'

    // whenever datapoints changes, we want to regenerate out points and send them down to plotter
    useEffect(() => {
        if (insertedProjections !== true) return
        if (datapoints === undefined) return

        let bounds = getBounds(datapoints)
        setTarget([bounds.centerX, bounds.centerY])
        setMaxSize(bounds.maxSize)
        calculateColorsAndDrawPoints()

        if (boundsSet == false) {
            config.scatterplot.set({
                cameraDistance: (bounds.maxSize * 1.4),
                minCameraDistance: (bounds.maxSize * 1.4) * (1 / 20),
                maxCameraDistance: (bounds.maxSize * 1.4) * 3,
                cameraTarget: [bounds.centerX, bounds.centerY],
            })
            setBoundsSet(true)
        }

    }, [insertedProjections, datapoints])

    if (reglInitialized && (points !== null)) {
        if (toolSelected == 'lasso') {
            config.scatterplot.setLassoOverride(true)
        } else {
            config.scatterplot.setLassoOverride(false)
        }

        // config.scatterplot.set({
        //     cameraDistance: (maxSize * 1.4),
        //     minCameraDistance: (maxSize * 1.4) * (1 / 20),
        //     maxCameraDistance: (maxSize * 1.4) * 3,
        //     cameraTarget: target,
        // })

        // TODO: manage selection....
        // if (unselectedPoints.length !== 0) {
        //     config.scatterplot.deselectIds(unselectedPoints)
        // }
    }

    // whenever points change, redraw
    useEffect(() => {
        if (reglInitialized && points !== null) {
            config.scatterplot.set({ pointColor: colorByOptions });
            config.scatterplot.draw(points)
        }
    }, [points])

    // whenever colorByFilterString change, redraw
    useEffect(() => {
        if (insertedProjections !== true) return
        if (datapoints === undefined) return
        calculateColorsAndDrawPoints()
    }, [colorByFilterString])

    const calculateColorsAndDrawPoints = () => {
        let colorByFilter = filters.find((a: any) => a.name == colorByFilterString)
        let colorByOptionsSave = colorByFilter.optionsSet.map((option: any) => option.color)
        setColorByOptions(colorByOptionsSave)

        points = datapoints!.map(datapoint => {
            let datapointColorByProp = colorByFilter.fetchFn(datapoint)[0]
            let datapointColorIndex = colorByFilter.optionsSet.findIndex((option: any) => option.name == datapointColorByProp)

            const visible = datapoint.visible ? 1 : 0
            return [datapoint.projection?.x, datapoint.projection?.y, visible, datapointColorIndex]
        })
        setPoints(points)
    }

    useEffect(() => {
        const resizeListener = () => {
            var canvas = document.getElementById("regl-canvas")
            var container = document.getElementById("regl-canvas-container")
            canvas!.style.width = container?.clientWidth + "px"
            canvas!.style.height = container?.clientHeight + "px"
        };
        window.addEventListener('resize', resizeListener);
        return () => {
            window.removeEventListener('resize', resizeListener);
        }
    }, [])

    function getRef(ref: any) {
        if (!ref) return;
        if (!reglInitialized && (points !== null)) {
            scatterplot(points,
                colorByOptions,
                {
                    pixelRatio: Math.min(1.5, window.devicePixelRatio),
                    canvas: ref,
                    deselectHandler: deselectHandler,
                    selectHandler: selectHandler,
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

    if (points === null) showLoading = true

    // how we set the cursor is a bit of a hack. if we have a custom cursor name
    // the cursor setting will fail, but our class will succeed in setting it
    // and vice versa
    return (
        <Box flex='1' cursor={cursor} className={cursor} id="regl-canvas-container" minWidth={0} marginTop="48px" width="800px">
            {(filters !== undefined) ?
                <Select pos="absolute" width={150} marginTop="10px" marginLeft="10px" value={colorByFilterString} onChange={newColorBy}>
                    {filters.map((filterb: any) => {
                        return (
                            <option key={filterb.name} value={filterb.name} >{filterb.name}</option>
                        )
                    })}
                </Select>
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

export default React.memo(ProjectionPlotter)
