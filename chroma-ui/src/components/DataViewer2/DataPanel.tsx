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
import { selectedDatapointsAtom, datapointsAtom, visibleDatapointsAtom, resourcesAtom, colsPerRowAtom, datapointModalIndexAtom, datapointModalOpenAtom } from './atoms';
import DatapointModal from './DatapointModal';
import ImageRenderer from './ImageRenderer';

interface DataPanelGridProps {
  datapoint: Datapoint
  index: number
}

const DataPanelGrid: React.FC<DataPanelGridProps> = ({ datapoint, index }) => {
  if (datapoint === undefined) return <></> // this is the case of not having a "full" row. the grid will still query for the item, but it does not exist

  const theme = useTheme()
  const bgColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const [resources] = useAtom(resourcesAtom)
  let [datapointModalIndex, updatedatapointModalIndex] = useAtom(datapointModalIndexAtom)
  const [datapointModalOpen, updatedatapointModalOpen] = useAtom(datapointModalOpenAtom)

  const uri = resources[datapoint.resource].uri

  // const [result, reexecuteQuery] = useQuery({
  //   query: ImageQuery,
  //   variables: { "identifier": uri, "thumbnail": true, "resolverName": 'filepath' },
  // });

  // const { data, fetching, error } = result;
  // if (error) return <p>Oh no... {error.message}</p>;

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
      borderWidth="2px"
      borderRadius={3}
    >
      <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%">
        <Flex direction="row" justifyContent="center" width="100%" minWidth={100} height={100}>
          <ImageRenderer imageUri={uri} annotations={datapoint.annotations} thumbnail={true} />
        </Flex>
        <Flex direction="row" justifyContent="space-evenly" alignItems="center" pl={1} borderRadius={5} bgColor={bgColor} ml="5px" mr="5px">
          <Flex alignItems="center" >
            <BsTag color='#666' />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.tags.length}</Text>
          </Flex>
          {/* <Flex alignItems="center" >
            <BiCategoryAlt color='#666' />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.label.data.categories[0].name}</Text>
          </Flex> */}
          {/* <Flex alignItems="center" >
            <BsLayers color='#666' style={{ transform: "rotate(-90deg)" }} />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.inference?.data.categories[0].name}</Text>
          </Flex> */}
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
  const [datapoints] = useAtom(datapointsAtom)
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

  const [datapoints, updatedatapoints] = useAtom(datapointsAtom)
  const [selectedDatapoints] = useAtom(selectedDatapointsAtom)
  const [visibleDatapoints, updatevisibleDatapoints] = useAtom(visibleDatapointsAtom)
  const [datapointModalOpen, updatedatapointModalOpen] = useAtom(datapointModalOpenAtom)

  const [colsPerRow, setcolsPerRow] = useAtom(colsPerRowAtom)

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

  // let modalDatapoint = 0
  // if (dps !== undefined) {
  //   // sending fns through itemData to react-window is stupid, but it is what it is
  //   dps?.map((dp, index) => {
  //     dp.triggerModal = () => triggerModal(index)
  //     dp.selected = (index === modalDatapointIndex)
  //   })
  //   modalDatapoint = dps[modalDatapointIndex]
  // }

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
