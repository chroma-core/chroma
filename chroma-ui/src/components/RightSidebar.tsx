import React from 'react';
import { Flex, Text, Box, Button, IconButton, useTheme, Divider, Badge, Spacer, useColorMode } from '@chakra-ui/react'
import { GiExpand } from 'react-icons/gi';
import { GrClose } from 'react-icons/gr';

import {
  Table,
  Thead,
  Tbody,
  Tfoot,
  Tr,
  Th,
  Td,
  TableCaption,
  TableContainer,
} from '@chakra-ui/react'
import { render } from '@testing-library/react';
import PopoverForm from './TagButton';

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
  const { colorMode } = useColorMode()
  const bgColor = { light: "#F3F5F6", dark: '#0c0c0b' }
  const bgColorCard = { light: "#E5E5E5", dark: '#222222' }

  return (
    <Flex 
      direction="column" 
      minWidth={300} 
      bg={bgColor[colorMode]}
      borderRight="1px"
      borderLeft="1px"
      borderColor={theme.colors.ch_gray.dark}
      maxHeight="100vh"
      overflowX="hidden"
      overflowY="scroll"
      p={3}
      css={{
        '&::-webkit-scrollbar': {
          width: '0px',
        },
      }}
      pt={16}>
      <Flex flex="row" align="center" justify="space-between" wrap="wrap" width="100%">
          <Button variant='ghost' size='sm'>{selectedPoints.length} selected</Button>
          <Flex>
            <Button variant='ghost' size='sm' colorScheme="blue" onClick={() => clearSelected()}>Clear</Button>
            <PopoverForm tagSelected={tagSelected}></PopoverForm>
          </Flex>
      </Flex>
      <Divider w="100%" pt={2}/>
      
          {selectedPoints.map(function(point){
            let metadata: Hash<string> = serverData[point][2]
            return (
              <Box 
                mt={3}
                bgColor={bgColorCard[colorMode]}
                pr={0} 
                borderRadius={5}
                pl={4}
                width={275}
                key={point}
                >
                <Flex flex="row" align="center" justify="space-between" wrap="wrap" width="100%" mb={3}>
                  <Text fontSize='sm' fontWeight={600} fontFamily="mono" width="200px">{point}</Text>
                  <Flex>
                    {/* <IconButton aria-label='Search database' icon={<GiExpand />} variant='ghost'/> */}
                    <IconButton aria-label='Clear' onClick={() => clearSelected([point])} icon={<GrClose />} variant='ghost'  />
                  </Flex>
                  <Spacer />
                  <TableContainer>
                    <Table variant='simple' size="sm" fontFamily="mono">
                      <Tbody>
                      {Object.entries(metadata).map(([key, val]) => {
                        return (
                        <Tr>
                          <Td p={1} pl={0} fontSize="xs">{key}</Td>
                          <Td p={1} fontSize="xs">{val}</Td>
                        </Tr>
                        )
                        })
                        /* <Tr>
                          <Td p={1} pl={0} fontSize="xs">feet</Td>
                          <Td p={1} fontSize="xs">centimetres (cm)</Td>
                        </Tr>
                        <Tr>
                          <Td p={1} pl={0} fontSize="xs">yards</Td>
                          <Td p={1} fontSize="xs">metres (m)</Td>
                        </Tr> */}
                      </Tbody>
                    </Table>
                  </TableContainer>
                  {/* <Divider bgColor={theme.colors.ch_gray.medium} />
                  <Flex pt={3} pb={3} wrap="wrap" rowGap={2} columnGap={2}>
                    <Badge variant='subtle' bgColor={theme.colors.ch_gray.medium} textTransform="none" fontFamily="mono" >Default</Badge>
                    <Badge variant='subtle' bgColor={theme.colors.ch_gray.medium} textTransform="none" fontFamily="mono" >Hello</Badge>
                    <Badge variant='subtle' bgColor={theme.colors.ch_gray.medium} textTransform="none" fontFamily="mono" >World</Badge>
                    <Badge variant='subtle' bgColor={theme.colors.ch_gray.medium} textTransform="none" fontFamily="mono" >asdflkklj88d</Badge>
                    <Badge variant='subtle' bgColor={theme.colors.ch_gray.medium} textTransform="none" fontFamily="mono" >12</Badge>
                  </Flex> */}
              </Flex>
            </Box>
            )
          })}
        {/*  */}
      
    </Flex>
  );
}

export default RightSidebar;
