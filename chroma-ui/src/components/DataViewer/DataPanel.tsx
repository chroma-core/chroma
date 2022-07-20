// @ts-nocheck
import React, { useEffect, useState } from 'react';
import { Spacer, Flex, Text, Box, useTheme, Divider, useColorModeValue, Skeleton, useDisclosure, Button, Modal, ModalBody, ModalContent, ModalHeader, ModalOverlay, ModalFooter, ModalCloseButton, Portal } from '@chakra-ui/react'
import { Grid as ChakraGrid, GridItem, Tag } from '@chakra-ui/react'
import { Table, Tbody, Tr, Td, TableContainer, Select, Center, Image } from '@chakra-ui/react'
import TagForm from './TagForm'
import Tags from './Tags'
import { Datapoint } from './DataViewTypes';
import { FixedSizeList as List, FixedSizeGrid as Grid } from "react-window";
import AutoSizer from "react-virtualized-auto-sizer"
import { useQuery } from 'urql'
import { Resizable } from 're-resizable'
import { Scrollbars } from 'react-custom-scrollbars'
import { BsTagFill, BsTag, BsLayers } from 'react-icons/bs'
import { BiCategoryAlt, BiCategory } from 'react-icons/bi'
import { datapointsAtom } from '../../atoms/datapointsAtom'
import { useAtom } from 'jotai'

export interface TagItem {
  left_id?: number
  right_id?: number
  tag: {
    id?: number
    name: string
  }
}

export interface ServerDataItem {
  id: number
  x: number
  y: number
  embedding: {
    id: number
    datapoint: {
      id: number
      dataset: {
        id: number
        name: string
      }
      label: {
        id: number
        data: any
      }
      resource: {
        id: number
        uri: string
      }
      tags: TagItem[]
    }
  }
}

interface DataPanelProps {
  selectedDatapointsIds: number[]
  setDatapointsAndRebuildFilters: (datapoints: ServerDataItem[]) => void
  filters: any[]
}

interface Hash<T> {
  [key: string]: T;
}

interface DataPanelGridProps {
  datapoint: any
  index: number
  totalLength: number
}

const ImageBytesQuery = `
  query getimage($identifer: String!) {
    mnistImage(identifier: $identifer) 
  }
`;

const DataPanelGrid: React.FC<DataPanelGridProps> = ({ datapoint }) => {
  if (datapoint === undefined) return <></> // this is the case of not having a "full" row. the grid will still query for the item, but it does not exist

  const theme = useTheme()
  const bgColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const [result, reexecuteQuery] = useQuery({
    query: ImageBytesQuery,
    variables: { "identifer": datapoint.resource.uri },
  });

  const { data, fetching, error } = result;
  if (error) return <p>Oh no... {error.message}</p>;

  return (
    <Box
      height={125}
      key={datapoint.id}
      borderColor={datapoint.selected ? "#09a6ff" : "rgba(0,0,0,0)"}
      onClick={datapoint.triggerModal}
      borderWidth="2px"
      borderRadius={3}
    >
      <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%">
        <Flex direction="row" justifyContent="center">
          {(data === undefined) ?
            <Skeleton width={100} height={100} />
            :
            <img width="100px" src={'data:image/jpeg;base64,' + data.mnistImage} />
          }
        </Flex>
        <Flex direction="row" justifyContent="space-evenly" alignItems="center" pl={1} borderRadius={5} bgColor={bgColor} ml="5px" mr="5px">
          <Flex alignItems="center" >
            <BsTag color='#666' />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.tags.length}</Text>
          </Flex>
          <Flex alignItems="center" >
            <BiCategoryAlt color='#666' />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.label.data.categories[0].name}</Text>
          </Flex>
          <Flex alignItems="center" >
            <BsLayers color='#666' style={{ transform: "rotate(-90deg)" }} />
            <Text fontWeight={600} fontSize="sm" color="#666">{datapoint.inference?.data.categories[0].name}</Text>
          </Flex>
        </Flex>
      </Flex >
    </Box >
  )
}

const DataPanelModal: React.FC<DataPanelGridProps> = ({ datapoint, setData, datapoints }) => {
  if (datapoint === undefined) return <></> // handle this case though we dont expect to run into it

  const theme = useTheme()
  const bgColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const [result, reexecuteQuery] = useQuery({
    query: ImageBytesQuery,
    variables: { "identifer": datapoint.resource.uri },
  });

  const { data, fetching, error } = result;
  if (error) return <p>Oh no... {error.message}</p>;

  return (
    <Box
      key={datapoint.id}
      width="100%"
      flexGrow={1}
    >
      <ChakraGrid templateColumns='repeat(3, 1fr)' gap={6} height="100%" py={3}>
        <GridItem colSpan={2} rowSpan={8} bgColor={bgColor}>
          <Center>
            {(data === undefined) ?
              <Skeleton width={200} height={200} />
              :
              <img width="200px" src={'data:image/jpeg;base64,' + data.mnistImage} />
            }
          </Center>
        </GridItem>
        <GridItem colSpan={1} rowSpan={8}>
          <Text fontWeight={600} pb={2}>Data</Text>
          <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"dpid"}>
                  <Td width="30%" fontSize="xs">Datapoint ID</Td>
                  <Td p={0} fontSize="xs">{datapoint.id}</Td>
                </Tr>
                <Tr key={"dataset"}>
                  <Td width="30%" fontSize="xs">Dataset</Td>
                  <Td p={0} fontSize="xs">{datapoint.dataset.name}</Td>
                </Tr>
                <Tr key={"quality"}>
                  <Td width="30%" fontSize="xs">Quality</Td>
                  <Td p={0} fontSize="xs">{(Math.exp(-parseFloat(datapoint.metadata_.distance_score)) * 100).toFixed(3)}</Td>
                </Tr>
              </Tbody>
            </Table>
          </TableContainer>
          <Flex pt={5} alignItems="center">
            <BiCategoryAlt color='#666' />
            <Text ml={1} fontWeight={600}>Label</Text>
          </Flex>
          <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"category"}>
                  <Td width="30%" fontSize="xs">Category</Td>
                  <Td p={0} fontSize="xs">{datapoint.label.data.categories[0].name}</Td>
                </Tr>
              </Tbody>
            </Table>
          </TableContainer>

          <Flex pt={5} alignItems="center">
            <BsLayers color='#666' style={{ transform: "rotate(-90deg)" }} />
            <Text ml={1} fontWeight={600}>Inference</Text>
          </Flex>
          <TableContainer>
            <Table variant='simple' size="sm">
              <Tbody>
                <Tr key={"category"}>
                  <Td width="30%" fontSize="xs">Category</Td>
                  <Td p={0} fontSize="xs">{datapoint.inference?.data.categories[0].name}</Td>
                </Tr>
              </Tbody>
            </Table>
          </TableContainer>

          <Flex pt={5} alignItems="center">
            <BsTag color='#666' />
            <Text ml={1} fontWeight={600}>Tags</Text>
          </Flex>
          <Flex mt={3}>
            <Tags setServerData={setData} tags={datapoint.tags} datapoints={datapoints} datapointId={datapoint.id} />
          </Flex>
        </GridItem>

      </ChakraGrid>
    </Box >
  )
}

// we have to go around the react-window interface in order to shove this data into the Cell element
var colsPerRow = 3

const Cell = ({ columnIndex, rowIndex, style, data }) => {
  let index = (rowIndex * colsPerRow) + columnIndex
  return (
    <div style={style}>
      <DataPanelGrid datapoint={data[index]} />
    </div>
  )
};

interface DatapointModalProps {
  datapoint: any
  isOpen: boolean
  onClose: () => void
  index: number
  totalLength: number
  setModalDatapointIndex: (index: number) => void
  setData: (datapoints: any) => void
  datapoints: datapoints
}

const DatapointModal: React.FC<DatapointModalProps> = ({ datapoint, isOpen, onClose, index, totalLength, setModalDatapointIndex, setData, datapoints }) => {
  const beginningOfList = (index === 0)
  const endOfList = (index === totalLength)
  const firstRow = ((index) < colsPerRow)
  const lastRow = ((index + 1) > (totalLength - colsPerRow))

  const theme = useTheme()
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')

  function handleKeyDown(event) {
    if ((event.keyCode === 37) && (!beginningOfList)) { // LEFT
      setModalDatapointIndex(index - 1)
    }
    if ((event.keyCode === 39) && (!endOfList)) { // RIGHT
      setModalDatapointIndex(index + 1)
    }
    if ((event.keyCode === 38) && (!firstRow)) { // UP
      setModalDatapointIndex(index - colsPerRow)
    }
    if ((event.keyCode === 40) && (!lastRow)) { // DOWN
      setModalDatapointIndex(index + colsPerRow)
    }
    if ((event.keyCode === 27)) { // ESC
      alert("esc!")
    }
  }

  return (
    <div
      onKeyDown={(e) => handleKeyDown(e)}
      tabIndex="0"
    >
      <Modal
        closeOnOverlayClick={false} // ESC also deselects... can we catch this
        isOpen={isOpen}
        onClose={onClose}
        autoFocus={true}
        closeOnEsc={true}
        blockScrollOnMount={false}
        onCloseComplete={() => setModalDatapointIndex(null)}
        width={300}
        size="full"
        variant="datapoint"
        scrollBehavior="inside"
      >
        <ModalContent bgColor={bgColor}>
          <ModalCloseButton />
          <ModalBody display="flex">
            <DataPanelModal datapoint={datapoint} datapoints={datapoints} setData={setData} />
          </ModalBody>
        </ModalContent>
      </Modal>
    </div>
  )
}

const DataPanel: React.FC<DataPanelProps> = ({ selectedDatapointsIds, setDatapointsAndRebuildFilters, filters }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const borderColorCards = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const [datapoints, setDatapoints] = useAtom(datapointsAtom);

  const { isOpen, onOpen, onClose } = useDisclosure()

  let [sortByFilterString, setSortByFilterString] = useState('Labels')
  let [sortByInvert, setSortByInvert] = useState(false)

  let [modalDatapointIndex, setModalDatapointIndex] = useState(null)

  const [resizeState, setResizeState] = useState({ width: 600, height: '100vh' })

  useEffect(() => {
    if (modalDatapointIndex === null) return
    gridRef.current.scrollToItem({
      rowIndex: Math.floor(modalDatapointIndex / colsPerRow)
    })
  }, [modalDatapointIndex])

  const gridRef = React.createRef();

  let datapointsToRender;
  let reactWindowListLength
  if (datapoints !== undefined) {
    datapointsToRender = datapoints.filter(dp => dp.visible == true)
    reactWindowListLength = datapointsToRender.length
  }

  if (selectedDatapointsIds.length > 0) {
    reactWindowListLength = selectedDatapointsIds.length
    datapointsToRender = datapoints.filter(dp => selectedDatapointsIds.includes(dp.id))
  }

  const newSortBy = (event: any) => {
    let str = event.target.value
    setSortByFilterString(str)
    let invert = (str.split("-")[1] === 'down')
    setSortByInvert(invert)
  }

  let validFilters
  if (filters !== undefined) {
    const noFilterList = ["Tags"]
    validFilters = filters.filter(f => !noFilterList.includes(f.name))

    let baseFilterName = sortByFilterString.split("-")[0]
    let sortByFilter = filters.find((a: any) => a.name == baseFilterName)
    var i = 0;
    datapointsToRender.sort(function (a, b) {
      let aVal = sortByFilter.fetchFn(a)[0]
      let bVal = sortByFilter.fetchFn(b)[0]

      if (aVal < bVal) return -1;
      if (bVal > aVal) return 1;
      return 0;
    })
    if (sortByInvert) datapointsToRender?.reverse()
  }

  function triggerModal(index) {
    setModalDatapointIndex(index)
    onOpen()
  }

  let modalDatapoint = 0
  if (datapointsToRender !== undefined) {
    // sending fns through itemData to react-window is stupid, but it is what it is
    datapointsToRender?.map((dp, index) => {
      dp.triggerModal = () => triggerModal(index)
      dp.selected = (index === modalDatapointIndex)
    })
    modalDatapoint = datapointsToRender[modalDatapointIndex]
  }

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
          <Text fontSize="sm" px={3} py={1}>{selectedDatapointsIds.length} selected</Text>
          {(filters !== undefined) ?
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
            : null}
        </Flex>

        <TagForm selectedDatapointsIds={selectedDatapointsIds} datapoints={datapoints} setDatapointsAndRebuildFilters={setDatapointsAndRebuildFilters} />

        <Divider w="100%" pt={0} />

        <Portal>
          <DatapointModal setData={setDatapointsAndRebuildFilters} datapoints={datapoints} index={modalDatapointIndex} totalLength={datapointsToRender?.length} datapoint={modalDatapoint} isOpen={isOpen} onClose={onClose} totalNum={1} setModalDatapointIndex={setModalDatapointIndex} />
        </Portal>

        {(datapoints !== undefined) ?
          <AutoSizer>
            {({ height, width }) => {
              let columnCount = Math.ceil((width / 150))
              colsPerRow = columnCount
              return (
                <Flex pt={2} style={{ width: width, height: height }}>
                  <Scrollbars autoHide style={{ width: width, height: height }}>
                    <Grid
                      ref={gridRef}
                      itemData={datapointsToRender}
                      columnCount={columnCount}
                      columnWidth={(width / columnCount) - colsPerRow} //offset for clipping, hardcoded
                      height={height - 110}
                      rowCount={Math.ceil(reactWindowListLength / colsPerRow) + 1} // extra row b/c its nice to scroll a bit past
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

