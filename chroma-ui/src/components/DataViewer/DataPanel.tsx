// @ts-nocheck
import React, { useState } from 'react';
import { Tag, Flex, Text, Box, CloseButton, IconButton, useTheme, Divider, Badge, Spacer, useColorMode, useColorModeValue } from '@chakra-ui/react'
import { GiExpand } from 'react-icons/gi';
import { BsTagFill, BsTag } from 'react-icons/bs';
import { Button, Table, Thead, Tbody, Tfoot, Tr, Th, Td, TableCaption, TableContainer } from '@chakra-ui/react'
import { render } from '@testing-library/react'
import TagForm from './TagForm'
import TagButton from './TagButton'
import Tags from './Tags'
import { Datapoint } from './DataViewTypes';
import { Resizable } from 're-resizable';
import { FixedSizeList as List, FixedSizeGrid as Grid } from "react-window";
import AutoSizer from "react-virtualized-auto-sizer";

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
  datapoints: Datapoint[]
  selectedPoints: number[]
}

interface Hash<T> {
  [key: string]: T;
}

const Row = ({ data, index, style }) => (
  < div style={style} >
    <Box
      mt={3}
      pr={4}
      pl={4}
      width="500px"
      key={data[index].id}
      borderWidth={1}
      borderColor="e5e5e5"
    >
      <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%" mb={3}>
        <Flex mb={2} direction="row" justify="space-between">
          <Text fontSize='sm' fontWeight={600}>{data[index].id}</Text>
        </Flex>
        <TableContainer>
          <Table variant='unstyled' size="sm">
            <Tbody>
              <Tr key={"category"}>
                <Td width="50%" p={0} pl={0} fontSize="xs">Category</Td>
                <Td p={0} fontSize="xs">{data[index].label.data.categories[0].name}</Td>
              </Tr>
              <Tr key={"dataset"}>
                <Td width="50%" p={0} pl={0} fontSize="xs">Dataset</Td>
                <Td p={0} fontSize="xs">{data[index].dataset.name}</Td>
              </Tr>
            </Tbody>
          </Table>
        </TableContainer>
        <Flex mt={3}>
          <Tags setServerData={() => { }} tags={data[index].tags} datapointId={data[index].id} />
        </Flex>
      </Flex >
    </Box >
  </div >
);

const DataPanel: React.FC<DataPanelProps> = ({ datapoints, selectedPoints }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const borderColorCards = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  // const [resizeState, setResizeState] = useState({ width: 1200, height: '100vh' })

  if (datapoints === undefined) {
    return (<>Loading</>)
  }

  let datapointsToRender = datapoints.filter(dp => dp.visible == true)
  let numDatapointsVisible = datapointsToRender.length
  let reactWindowListLength = datapointsToRender.length

  if (selectedPoints.length > 0) {
    reactWindowListLength = selectedPoints.length
    datapoints = datapoints.filter(dp => selectedPoints.includes(dp.id)) // i dont know where this 1 offset came from, but shipping it for now
  }

  return (
    // <Resizable
    //   size={{ width: resizeState.width, height: resizeState.height }}
    //   onResizeStop={(e, direction, re2f, d) => {
    //     setResizeState({
    //       width: resizeState.width + d.width,
    //       height: resizeState.height + d.height,
    //     });
    //   }}
    // >
    <Flex
      direction="column"
      width={500}
      bg={bgColor}
      borderRight="1px"
      borderLeft="1px"
      borderColor={borderColor}
      height="100vh"
      overflowX="hidden"
      overflowY="scroll"
      css={{
        '&::-webkit-scrollbar': {
          width: '0px',
        },
      }}
      pt={12}>
      <p>{numDatapointsVisible} visible</p>
      <p>{selectedPoints.length} selected</p>
      <Divider w="100%" />

      <AutoSizer>
        {({ height, width }) =>
          <List
            // columnCount={3}
            itemData={datapointsToRender}
            itemSize={100}
            // columnWidth={resizeState.width / 3}
            height={height}
            itemCount={reactWindowListLength}
            // rowCount={1000}
            // rowHeight={35}
            width={800}
          >
            {Row}
          </List>
        }
      </AutoSizer>

    </Flex >
    // </Resizable>
  )
}

export default DataPanel


// {datapoints.map(function (datapoint) {
//   let category = datapoint.label.data.categories[0].name
//   let dataset = datapoint.dataset

//   return (
//     <Box
//       mt={3}
//       pr={4}
//       pl={4}
//       width="300px"
//       key={datapoint.id}
//       borderWidth={1}
//       borderColor={borderColorCards}
//     >
//       <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%" mb={3}>
//         <Flex mb={2} direction="row" justify="space-between">
//           <Text fontSize='sm' fontWeight={600}>{datapoint.id}</Text>
//           <CloseButton
//             size='sm'
//             opacity={0.4}
//             _hover={{ opacity: 1 }}
//             // onClick={() => clearSelected([point])}
//             my={0} />
//         </Flex>
//         <TableContainer>
//           <Table variant='unstyled' size="sm">
//             <Tbody>
//               <Tr key={"category"}>
//                 <Td width="50%" p={0} pl={0} fontSize="xs">Category</Td>
//                 <Td p={0} fontSize="xs">{category}</Td>
//               </Tr>
//               <Tr key={"dataset"}>
//                 <Td width="50%" p={0} pl={0} fontSize="xs">Dataset</Td>
//                 <Td p={0} fontSize="xs">{dataset.name}</Td>
//               </Tr>
//             </Tbody>
//           </Table>
//         </TableContainer>
//         {/* <Flex mt={3}>
//           <Tags setServerData={setServerData} tags={serverData[point].embedding.datapoint.tags} datapointId={datapoint.id} />
//         </Flex> */}
// </Flex >
//     </Box >
//   )
// })} 