// @ts-nocheck
import React from 'react';
import { Tag, Flex, Text, Box, CloseButton, IconButton, useTheme, Divider, Badge, Spacer, useColorMode, useColorModeValue } from '@chakra-ui/react'
import { GiExpand } from 'react-icons/gi';
import { BsTagFill, BsTag } from 'react-icons/bs';

import { Button, Table, Thead, Tbody, Tfoot, Tr, Th, Td, TableCaption, TableContainer } from '@chakra-ui/react'
import { render } from '@testing-library/react'
import TagForm from './TagForm'
import TagButton from './TagButton'
import Tags from './Tags'

interface RightSidebarProps {
  selectedPoints: []
  tagSelected: () => void
  clearSelected: any
  serverData: []
  setServerData: () => void
}

interface Hash<T> {
  [key: string]: T;
}

const RightSidebar: React.FC<RightSidebarProps> = ({ setServerData, selectedPoints, tagSelected, clearSelected, serverData }) => {
  const theme = useTheme();
  const bgColor = useColorModeValue("#FFFFFF", '#0c0c0b')
  const bgColorCard = useColorModeValue("#E5E5E5", '#222222')
  const borderColor = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)
  const borderColorCards = useColorModeValue(theme.colors.ch_gray.light, theme.colors.ch_gray.dark)

  const deselectButtonOpacity = (selectedPoints.length > 0) ? 0.4 : 0

  return (
    <Flex
      direction="column"
      minWidth={300}
      maxWidth={300}
      bg={bgColor}
      borderRight="1px"
      borderLeft="1px"
      borderColor={borderColor}
      maxHeight="100vh"
      overflowX="hidden"
      overflowY="scroll"
      css={{
        '&::-webkit-scrollbar': {
          width: '0px',
        },
      }}
      pt={12}>
      <Flex flex="row" wrap="wrap" width="100%" py={1} ml={3} pb={0}>
        <Text fontWeight={600} fontSize={14} lineHeight="2rem" mx={3}>{selectedPoints.length} selected</Text>
        <CloseButton
          size='sm'
          opacity={deselectButtonOpacity}
          _hover={{ opacity: 1 }}
          onClick={() => clearSelected()}
          my={1} />
        <Flex>
        </Flex>
      </Flex>
      <TagForm setServerData={setServerData} selectedPoints={selectedPoints} serverData={serverData} />

      <Divider w="100%" />
      {selectedPoints.map(function (point) {
        let datapointId = serverData[point].embedding.datapoint.id
        let category = JSON.parse(serverData[point].embedding.datapoint.label.data).categories[0].name
        let dataset = serverData[point].embedding.datapoint.dataset.name
        return (
          <Box
            mt={3}
            pr={4}
            pl={4}
            width="100%"
            key={point}
            borderBottomWidth={1}
            borderColor={borderColorCards}
          >
            <Flex direction="column" flex="row" justify="space-between" wrap="wrap" width="100%" mb={3}>

              <Flex mb={2} direction="row" justify="space-between">
                <Text fontSize='sm' fontWeight={600}>{datapointId}</Text>
                <CloseButton
                  size='sm'
                  opacity={0.4}
                  _hover={{ opacity: 1 }}
                  onClick={() => clearSelected([point])}
                  my={0} />
              </Flex>

              <TableContainer>
                <Table variant='unstyled' size="sm">
                  <Tbody>
                    <Tr key={"category"}>
                      <Td width="50%" p={0} pl={0} fontSize="xs">Category</Td>
                      <Td p={0} fontSize="xs">{category}</Td>
                    </Tr>
                    <Tr key={"dataset"}>
                      <Td width="50%" p={0} pl={0} fontSize="xs">Dataset</Td>
                      <Td p={0} fontSize="xs">{dataset}</Td>
                    </Tr>
                    {/* {Object.entries(metadata).map(([key, val]) => {
                      return (
                        <Tr key={key}>
                          <Td width="50%" p={0} pl={0} fontSize="xs">{key}</Td>
                          <Td p={0} fontSize="xs">{val}</Td>
                        </Tr>
                      )
                    })
                    } */}
                  </Tbody>
                </Table>
              </TableContainer>
              <Flex mt={3}>
                <Tags setServerData={setServerData} tags={serverData[point].embedding.datapoint.tags} datapointId={datapointId} />
              </Flex>
            </Flex >
          </Box >
        )
      })}
    </Flex >
  )
}

export default RightSidebar
