// @ts-nocheck
import React from 'react';
import { Flex, Text, Box, CloseButton, IconButton, useTheme, Divider, Badge, Spacer, useColorMode, useColorModeValue } from '@chakra-ui/react'
import { GiExpand } from 'react-icons/gi';
import { GrClose } from 'react-icons/gr';

import { Table, Thead, Tbody, Tfoot, Tr, Th, Td, TableCaption, TableContainer } from '@chakra-ui/react'
import { render } from '@testing-library/react'
import PopoverForm from './TagButton'

interface RightSidebarProps {
  selectedPoints: []
  tagSelected: () => void
  clearSelected: any
  serverData: []
}

interface Hash<T> {
  [key: string]: T;
}

const RightSidebar: React.FC<RightSidebarProps> = ({ selectedPoints, tagSelected, clearSelected, serverData }) => {
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
      <Flex flex="row" wrap="wrap" width="100%" py={1}>
        <Text fontWeight={600} fontSize={14} lineHeight="2rem" mx={3}>{selectedPoints.length} selected</Text>
        <CloseButton
          size='sm'
          opacity={deselectButtonOpacity}
          _hover={{ opacity: 1 }}
          onClick={() => clearSelected()}
          my={1} />
        <Flex>
          {/* <PopoverForm tagSelected={tagSelected}></PopoverForm> */}
        </Flex>
      </Flex>
      <Divider w="100%" />
      {selectedPoints.map(function (point) {
        console.log('serverdata point', serverData[point])
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
                <Text fontSize='sm' fontWeight={600}>{point}</Text>
                <CloseButton
                  size='sm'
                  opacity={0.4}
                  _hover={{ opacity: 1 }}
                  onClick={() => clearSelected([point])}
                  my={0} />
                {/* <IconButton aria-label='Clear' onClick={() => clearSelected([point])} icon={<GrClose />} variant='ghost'  /> */}
              </Flex>
              <TableContainer>
                <Table variant='unstyled' size="sm">
                  <Tbody>
                    <Tr key="class">
                      <Td width="50%" p={0} pl={0} fontSize="xs">Class</Td>
                      <Td p={0} fontSize="xs">{category}</Td>
                    </Tr>
                    <Tr key="dataset">
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
            </Flex >
          </Box >
        )
      })}
      {/*  */}

    </Flex >
  )
}

export default RightSidebar
