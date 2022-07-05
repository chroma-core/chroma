// @ts-nocheck
import React, { useState } from 'react';
import { Tag, Flex, Text, Box, Spinner, IconButton, useTheme, Divider, Spacer, useColorMode, useColorModeValue, Skeleton } from '@chakra-ui/react'
import { GiExpand } from 'react-icons/gi';
import { BsTagFill, BsTag } from 'react-icons/bs';
import { Button, Table, Thead, Tbody, Tfoot, Tr, Th, Td, TableCaption, TableContainer } from '@chakra-ui/react'
import { render } from '@testing-library/react'
import TagForm from './TagForm'
import TagButton from './TagButton'
import Tags from './Tags'
import { Datapoint } from './DataViewTypes';
import { FixedSizeList as List, FixedSizeGrid as Grid } from "react-window";
import AutoSizer from "react-virtualized-auto-sizer";
import { useQuery } from 'urql';

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
  selectedDatapointsIds: number[]
  setDatapointsAndRebuildFilters: (datapoints: ServerDataItem[]) => void
}

interface Hash<T> {
  [key: string]: T;
}

interface DataPanelRowProps {
  datapoint: any
}

const ImageBytesQuery = `
  query getimage($identifer: String!) {
    mnistImage(identifier: $identifer) 
  }
`;

const DataPanelRow: React.FC<DataPanelRowProps> = ({ datapoint }) => {

  const [result, reexecuteQuery] = useQuery({
    query: ImageBytesQuery,
    variables: { "identifer": datapoint.resource.uri },
  });

  const { data, fetching, error } = result;
  if (error) return <p>Oh no... {error.message}</p>;

  return (
    <Box
      mt={3}
      pr={4}
      pl={4}
      width={300}
      key={datapoint.id}
      borderBottomWidth={1}
      borderColor="e5e5e5"
    >
      <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%" mb={3}>
        <Flex mb={2} direction="row" justify="space-between">
          {(data === undefined) ?
            <Skeleton width={100} height={100} />
            :
            <img width="100px" src={'data:image/jpeg;base64,' + data.mnistImage} />
          }
        </Flex>
        <TableContainer>
          <Table variant='unstyled' size="sm">
            <Tbody>
              <Tr key={"dpid"}>
                <Td width="20%" p={0} pl={0} fontSize="xs">Datapoint ID</Td>
                <Td p={0} fontSize="xs">{datapoint.id}</Td>
              </Tr>
              <Tr key={"category"}>
                <Td width="20%" p={0} pl={0} fontSize="xs">Category</Td>
                <Td p={0} fontSize="xs">{datapoint.label.data.categories[0].name}</Td>
              </Tr>
              <Tr key={"dataset"}>
                <Td width="20%" p={0} pl={0} fontSize="xs">Dataset</Td>
                <Td p={0} fontSize="xs">{datapoint.dataset.name}</Td>
              </Tr>
            </Tbody>
          </Table>
        </TableContainer>
        <Flex mt={3}>
          <Tags setServerData={() => { }} tags={datapoint.tags} datapointId={datapoint.id} />
        </Flex>
      </Flex >
    </Box >
  )
}

const Row = ({ data, index, style }) => (
  <div style={style} >
    <DataPanelRow datapoint={data[index]} />
  </div>
);

const DataPanel: React.FC<DataPanelProps> = ({ datapoints, selectedDatapointsIds, setDatapointsAndRebuildFilters }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const borderColorCards = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

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

  return (
    <Flex
      direction="column"
      width={300}
      minWidth={300}
      maxWidth={300}
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
      pt={14}>
      <Flex key="buttons" px={3} justifyContent="space-between" alignContent="center">
        <Text fontWeight={600}>Inspect</Text>
        <Text fontSize="sm">{selectedDatapointsIds.length} selected</Text>
      </Flex>

      <TagForm selectedDatapointsIds={selectedDatapointsIds} datapoints={datapoints} setDatapointsAndRebuildFilters={setDatapointsAndRebuildFilters} />

      <Divider w="100%" pt={2} />

      {(datapoints !== undefined) ?
        <AutoSizer>
          {({ height, width }) =>
            <List
              itemData={datapointsToRender}
              itemSize={250}
              height={height}
              itemCount={reactWindowListLength}
              width={300}
            >
              {Row}
            </List>
          }
        </AutoSizer>
        : null}
    </Flex >
  )
}

export default DataPanel
