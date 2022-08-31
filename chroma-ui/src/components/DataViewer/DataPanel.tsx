import React, { useEffect, useState } from 'react';
import { Spacer, Flex, Text, Box, useTheme, Divider, useColorModeValue, Skeleton, useDisclosure, Button, Modal, ModalBody, ModalContent, ModalHeader, ModalOverlay, ModalFooter, ModalCloseButton, Portal } from '@chakra-ui/react'
import { Grid as ChakraGrid, GridItem, Tag } from '@chakra-ui/react'
import { Table, Tbody, Tr, Td, TableContainer, Select, Center, Image } from '@chakra-ui/react'
import TagForm from './TagForm'
import Tags from './Tags'
import { Datapoint } from './types';
import { FixedSizeList as List, FixedSizeGrid as Grid } from "react-window";
import AutoSizer from "react-virtualized-auto-sizer"
import { useQuery } from 'urql'
import { Resizable } from 're-resizable'
import { Scrollbars } from 'react-custom-scrollbars'
import { BsTagFill, BsTag, BsLayers } from 'react-icons/bs'
import { BiCategoryAlt, BiCategory } from 'react-icons/bi'
import { useAtom } from 'jotai';
import { selectedDatapointsAtom, context__datapointsAtom, visibleDatapointsAtom, context__resourcesAtom, colsPerRowAtom, datapointModalIndexAtom, datapointModalOpenAtom, contextObjectSwitcherAtom, DataType, globalSelectedDatapointsAtom, globalVisibleDatapointsAtom, globalDatapointAtom, globalResourcesAtom, object__categoriesAtom, labelSelectedDatapointsAtom, hoverToHighlightInPlotterDatapointIdAtom } from './atoms';
import DatapointModal from './DatapointModal';
import ImageRenderer from './ImageRenderer';

interface DataPanelGridProps {
  datapoint: Datapoint
  index: number
}

function uniq_fast(a: any) {
  var seen = {};
  var out = [];
  var len = a.length;
  var j = 0;
  for (var i = 0; i < len; i++) {
    var item = a[i];
    // @ts-ignore
    if (seen[item] !== 1) {
      // @ts-ignore
      seen[item] = 1;
      out[j++] = item;
    }
  }
  return out;
}

export const DataPanelGrid: React.FC<DataPanelGridProps> = ({ datapoint, index }) => {
  if (datapoint === undefined) return <></> // this is the case of not having a "full" row. the grid will still query for the item, but it does not exist

  const theme = useTheme()
  const bgColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const [resources] = useAtom(globalResourcesAtom)
  let [datapointModalIndex, updatedatapointModalIndex] = useAtom(datapointModalIndexAtom)
  const [datapointModalOpen, updatedatapointModalOpen] = useAtom(datapointModalOpenAtom)
  const [contextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)
  const [labelCategories] = useAtom(object__categoriesAtom)

  const [hoverPoint, setHoverPoint] = useAtom(hoverToHighlightInPlotterDatapointIdAtom)

  const uri = resources[datapoint.resource_id].uri

  const triggerModal = () => {
    updatedatapointModalIndex(index)
    updatedatapointModalOpen(true)
  }

  return (
    <Box
      height={125}
      key={datapoint.id}
      borderColor={((datapointModalIndex == index) && datapointModalOpen) ? "#09a6ff" : "rgba(0,0,0,0)"}
      onClick={triggerModal}
      onMouseEnter={() => setHoverPoint(datapoint.id)}
      onMouseLeave={() => setHoverPoint(undefined)}
      _hover={{
        borderColor: ((datapointModalIndex == index) && datapointModalOpen) ? "#09a6ff" : "#87d4ff"
      }}
      borderWidth="2px"
      borderRadius={3}
    >
      <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%">
        <Flex direction="row" justifyContent="center" width="100%" minWidth={100} height={100}>
          <ImageRenderer imageUri={uri} annotations={datapoint.annotations} thumbnail={true} />
        </Flex>
        <Flex direction="row" justifyContent="space-evenly" alignItems="center" pl={1} borderRadius={5} bgColor={bgColor} ml="5px" mr="5px">
          <Flex alignItems="center">
            <BsTag color='#666' />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.tag_ids.length}</Text>
          </Flex>
          {(contextObjectSwitcher == DataType.Object) ?
            <Flex>
              <Text fontWeight={600} fontSize="sm" color="#666">{labelCategories[datapoint.annotations[0].category_id].name}</Text>
            </Flex>
            : null}
        </Flex>
      </Flex >
    </Box >
  )
}

interface CellProps {
  columnIndex: number
  rowIndex: number
  style: any
  data: any
}

const Cell: React.FC<CellProps> = ({ columnIndex, rowIndex, style, data }) => {
  const [colsPerRow] = useAtom(colsPerRowAtom)
  const [datapoints] = useAtom(globalDatapointAtom)
  let index = (rowIndex * colsPerRow) + columnIndex

  return (
    <div style={style}>
      <DataPanelGrid datapoint={datapoints[data[index]]} index={index} />
    </div>
  )
};

const DataPanel: React.FC = () => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const [datapoints] = useAtom(globalDatapointAtom)
  const [selectedDatapoints] = useAtom(globalSelectedDatapointsAtom)
  const [visibleDatapoints] = useAtom(globalVisibleDatapointsAtom)
  const [datapointModalOpen, updatedatapointModalOpen] = useAtom(datapointModalOpenAtom)
  const [contextObjectSwitcher, updatecontextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)

  const [colsPerRow, setcolsPerRow] = useAtom(colsPerRowAtom)

  const [selectedContextDatapoints, setSelectedContextDatapoints] = useAtom(selectedDatapointsAtom)
  const [selectedObjectDatapoints, setSelectedObjectDatapoints] = useAtom(labelSelectedDatapointsAtom)

  const { isOpen, onOpen, onClose } = useDisclosure()

  // let [sortByFilterString, setSortByFilterString] = useState('Labels')
  // let [sortByInvert, setSortByInvert] = useState(false)
  let [datapointModalIndex, setdatapointModalIndex] = useAtom(datapointModalIndexAtom)

  const [resizeState, setResizeState] = useState({ width: 600, height: '100vh' })

  const gridRef = React.createRef<Grid>();

  useEffect(() => {
    if ((datapointModalIndex === null) || (gridRef.current === null)) return
    gridRef!.current!.scrollToItem({
      rowIndex: Math.floor(datapointModalIndex / colsPerRow)
    })
  }, [datapointModalIndex])

  // const newSortBy = (event: any) => {
  //   let str = event.target.value
  //   setSortByFilterString(str)
  //   let invert = (str.split("-")[1] === 'down')
  //   setSortByInvert(invert)
  // }
  // let validFilters
  // if (filters !== undefined) {
  //   const noFilterList = ["Tags"]
  //   validFilters = filters.filter(f => !noFilterList.includes(f.name))

  //   let baseFilterName = sortByFilterString.split("-")[0]
  //   let sortByFilter = filters.find((a: any) => a.name == baseFilterName)
  //   var i = 0;
  //   datapointsToRender.sort(function (a, b) {
  //     let aVal = sortByFilter.fetchFn(a)[0]
  //     let bVal = sortByFilter.fetchFn(b)[0]

  //     if (aVal < bVal) return -1;
  //     if (bVal > aVal) return 1;
  //     return 0;
  //   })
  //   if (sortByInvert) datapointsToRender?.reverse()
  // }

  var dps: number[] = []
  if (selectedDatapoints.length > 0) {
    dps = selectedDatapoints//.filter( ( el ) => visibleDatapoints.includes( el ) );
  }
  else dps = visibleDatapoints

  const showRelated = () => {
    if (contextObjectSwitcher == DataType.Context) {
      // build a list of the associated records
      var objectDatapointIds: number[] = []
      dps.forEach(dp => {
        objectDatapointIds.push(...datapoints[dp].object_datapoint_ids!)
      })
      objectDatapointIds = uniq_fast(objectDatapointIds) // remove dupes

      // select them
      setSelectedObjectDatapoints(objectDatapointIds)

      // switch to the other view
      updatecontextObjectSwitcher(DataType.Object)
    } else {
      // build a list of the associated records
      var objectDatapointIds2: number[] = []
      dps.forEach(dp => {
        objectDatapointIds2.push(datapoints[dp].source_datapoint_id!)
      })
      objectDatapointIds2 = uniq_fast(objectDatapointIds2) // remove dupes

      // select them
      setSelectedContextDatapoints(objectDatapointIds2)

      // switch to the other view
      updatecontextObjectSwitcher(DataType.Context)
    }

  }

  // dps.sort(function (a, b) { return a - b });

  return (
    <Resizable
      size={{ width: resizeState.width, height: resizeState.height }}
      minWidth={400}
      onResizeStop={(e, direction, re2f, d) => {
        setResizeState({
          width: resizeState.width + d.width,
          height: resizeState.height + d.height,
        });
      }}
      enable={{ top: false, right: false, bottom: false, left: true, topRight: false, bottomRight: false, bottomLeft: false, topLeft: false }}
    >
      <Flex
        direction="column"
        width="100%"
        bg={bgColor}
        borderRight="1px"
        borderLeft="1px"
        borderColor={borderColor}
        height="100vh"
        overflowX="hidden"
        overflowY="hidden"
        pt={14}
      >
        <Flex key="buttons" px={3} justifyContent="space-between" alignContent="center">
          <Text fontWeight={600}>Inspect</Text>
          <Button onClick={showRelated} variant="ghost" size="sm">Show {(contextObjectSwitcher == DataType.Context) ? "related objects" : "source contexts"}</Button>
          <Text fontSize="sm" px={3} py={1}>{dps.length} selected</Text>
          {/* {(filters !== undefined) ?
            <Select variant="ghost" size="xs" fontWeight={600} width="120px" value={sortByFilterString} onChange={newSortBy}>
              {validFilters.map((filterb: any) => {
                return (
                  <React.Fragment key={filterb.name}>
                    <option key={filterb.name + "-up"} value={filterb.name + "-up"}>{filterb.name} - Up</option>
                    <option key={filterb.name + "-down"} value={filterb.name + "-down"}>{filterb.name} - Down</option>
                  </React.Fragment>
                )
              })}
            </Select>
            : null} */}
        </Flex>

        <TagForm />

        <Divider w="100%" pt={0} />

        <Portal>
          <DatapointModal totalLength={dps?.length} isOpen={datapointModalOpen} />
        </Portal>

        {(dps.length > 0) ?
          <AutoSizer>
            {({ height, width }) => {
              let columnCount = Math.ceil((width / 150))
              setcolsPerRow(columnCount)
              return (
                <Flex pt={2} style={{ width: width, height: height }}>
                  <Scrollbars autoHide style={{ width: width, height: height }}>
                    <Grid
                      ref={gridRef}
                      itemData={dps}
                      columnCount={columnCount}
                      columnWidth={(width / columnCount) - colsPerRow} //offset for clipping, hardcoded
                      height={height - 110}
                      rowCount={Math.ceil(dps.length / colsPerRow) + 1} // extra row b/c its nice to scroll a bit past
                      rowHeight={125}
                      width={width}
                    >
                      {Cell}
                    </Grid>
                  </Scrollbars>
                </Flex>
              )
            }}
          </AutoSizer>
          : null}
      </Flex>
    </Resizable>
  )
}

export default DataPanel
